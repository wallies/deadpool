"""
Deadpool
========

A process pool executor that is really hard to kill.

This module implements the ``concurrent.futures.Executor`` interface with
three design ideas borrowed (conceptually) from ClickHouse's "Silk" fiber
runtime:

1. **A single event-driven manager loop.** Instead of one thread per running
   task that polls a pipe every 200ms, there is exactly one manager thread
   that waits on *all* task pipes and process sentinels at once using
   ``multiprocessing.connection.wait``. It wakes only when something actually
   happens (a result arrives, a process dies, a deadline passes, or a new task
   is submitted). This is the Silk "wait on events, never poll" idea mapped
   onto the stdlib.

2. **A central deadline heap.** Task timeouts live in a single
   deadline-ordered heap serviced by the same manager loop, instead of a
   ``threading.Timer`` thread per task. This mirrors Silk's deadline-ordered
   sleep tree.

3. **Optional process reuse.** ``max_tasks_per_child`` (default ``1``, i.e.
   the original "fresh process per task" behaviour) lets a subprocess handle
   several tasks before being retired, amortizing spawn cost. This is the
   process-level analogue of Silk's "no per-operation setup cost in steady
   state". Resilience is preserved: a worker is still discarded on timeout,
   on unexpected death (OOM/external kill), and once it hits its task budget.
"""
import os
import signal
import time
import heapq
import multiprocessing as mp
from multiprocessing.connection import wait
import concurrent.futures
from concurrent.futures import Executor
import threading
from queue import Queue, Empty, Full
from collections import deque
from typing import Callable, Optional
import logging

import psutil


__version__ = "2022.9.2"
logger = logging.getLogger(__name__)


class Future(concurrent.futures.Future):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._pid: Optional[int] = None
        self.pid_callback = None

    @property
    def pid(self):
        return self._pid

    @pid.setter
    def pid(self, value):
        self._pid = value
        if self.pid_callback:
            try:
                self.pid_callback(self._pid)
            except Exception:  # pragma: no cover
                logger.exception("Error calling pid_callback")

    def add_pid_callback(self, fn):
        self.pid_callback = fn


class TimeoutError(concurrent.futures.TimeoutError):
    ...


class ProcessError(mp.ProcessError):
    ...


class PoolClosed(Exception):
    ...


class _Worker:
    """A subprocess plus its duplex pipe. May run more than one task over its
    lifetime when ``max_tasks_per_child`` allows reuse."""

    __slots__ = ("process", "conn", "tasks_run", "current", "fut")

    def __init__(self, process, conn):
        self.process = process
        self.conn = conn
        self.tasks_run = 0
        self.current = None  # the in-flight task tuple, or None when idle
        self.fut = None      # the in-flight Future, or None when idle


class Deadpool(Executor):
    def __init__(
        self,
        max_workers: Optional[int] = None,
        mp_context=None,
        initializer=None,
        initargs=(),
        finalizer=None,
        finalargs=(),
        max_tasks_per_child: Optional[int] = 1,
    ) -> None:
        super().__init__()

        if not mp_context:
            mp_context = "forkserver"

        if isinstance(mp_context, str):
            mp_context = mp.get_context(mp_context)

        if max_tasks_per_child is not None and max_tasks_per_child < 1:
            raise ValueError("max_tasks_per_child must be >= 1 or None")

        self.ctx = mp_context
        self.initializer = initializer
        self.initargs = initargs
        self.finitializer = finalizer
        self.finitargs = finalargs
        self.pool_size = max_workers or len(os.sched_getaffinity(0))
        self.max_tasks_per_child = max_tasks_per_child
        self.closed = False

        # Backpressure: submit() blocks once 100 tasks are waiting, exactly as
        # the original bounded submitted_jobs queue did.
        self._pending: "Queue" = Queue(maxsize=100)
        # Tasks drained from _pending that are waiting for a free slot.
        self._unassigned: "deque" = deque()
        # Live workers (idle or busy), capped at pool_size.
        self._workers: list = []
        # Processes on their way out: sentinel fd -> Process.
        self._reaping: dict = {}
        # Deadline heap of (deadline, seq, worker, fut) for timeouts.
        self._deadlines: list = []
        self._seq = 0

        # Self-pipe used to wake the manager loop out of wait() when a new task
        # is submitted or the pool is shutting down.
        self._wake_r, self._wake_w = os.pipe()
        os.set_blocking(self._wake_r, False)

        self.manager_thread: Optional[threading.Thread] = None

    # -- public API ---------------------------------------------------------

    def submit(self, __fn: Callable, *args, timeout=None, **kwargs) -> Future:
        if self.closed:
            raise PoolClosed("The pool is closed. No more tasks can be submitted.")

        fut = Future()
        self._pending.put((__fn, args, kwargs, timeout, fut))
        self._nudge()
        return fut

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
        self.closed = True
        self._nudge()
        if wait and self.manager_thread is not None:
            self.manager_thread.join()
        return super().shutdown(wait, cancel_futures=cancel_futures)

    def __enter__(self):
        self.manager_thread = threading.Thread(target=self._manage, daemon=True)
        self.manager_thread.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown(wait=True)
        return False

    # -- internals ----------------------------------------------------------

    def _nudge(self):
        try:
            os.write(self._wake_w, b"x")
        except (BlockingIOError, OSError):  # pragma: no cover
            pass

    def _drain_pending(self):
        while True:
            try:
                self._unassigned.append(self._pending.get_nowait())
            except Empty:
                break

    def _spawn_worker(self) -> _Worker:
        parent_conn, child_conn = mp.Pipe()  # duplex
        p = self.ctx.Process(
            target=_worker_loop,
            args=(
                child_conn,
                self.initializer,
                self.initargs,
                self.finitializer,
                self.finitargs,
                self.max_tasks_per_child,
            ),
        )
        p.start()
        child_conn.close()  # only the child keeps its end
        worker = _Worker(p, parent_conn)
        self._workers.append(worker)
        return worker

    def _get_idle_worker(self) -> Optional[_Worker]:
        budget = self.max_tasks_per_child
        for w in self._workers:
            if w.current is None and w.process.is_alive():
                if budget is None or w.tasks_run < budget:
                    return w
        return None

    def _dispatch(self, worker: _Worker, task):
        fn, args, kwargs, timeout, fut = task
        worker.current = task
        worker.fut = fut
        try:
            worker.conn.send((fn, args, kwargs))
        except (BrokenPipeError, OSError):  # worker died before dispatch
            worker.current = None
            worker.fut = None
            if not fut.done():
                fut.set_exception(ProcessError("Worker died before task dispatch"))
            self._reap(worker)
            return
        fut.pid = worker.process.pid
        if timeout:
            self._seq += 1
            deadline = time.monotonic() + timeout
            heapq.heappush(self._deadlines, (deadline, self._seq, worker, fut))

    def _assign(self):
        while self._unassigned:
            worker = self._get_idle_worker()
            if worker is None:
                if len(self._workers) < self.pool_size:
                    worker = self._spawn_worker()
                else:
                    break  # no free slot; try again after the next event
            task = self._unassigned.popleft()
            fut = task[4]
            if not fut.set_running_or_notify_cancel():
                # Future was cancelled before it started; leave the worker idle
                # so it can pick up the next task.
                continue
            self._dispatch(worker, task)

    def _on_result(self, worker: _Worker):
        try:
            tag, value = worker.conn.recv()
        except (EOFError, OSError):
            # Worker died while we expected a result.
            self._on_death(worker)
            return

        fut = worker.fut
        worker.current = None
        worker.fut = None
        worker.tasks_run += 1

        if fut is not None and not fut.done():
            if tag == "exc":
                fut.set_exception(value)
            else:
                fut.set_result(value)

        budget = self.max_tasks_per_child
        if budget is not None and worker.tasks_run >= budget:
            # Worker exits on its own once it hits its task budget.
            self._reap(worker)
        # otherwise keep it idle for reuse

    def _on_death(self, worker: _Worker):
        fut = worker.fut
        worker.current = None
        worker.fut = None
        if fut is not None and not fut.done():
            exitcode = worker.process.exitcode
            try:
                signame = signal.strsignal(-exitcode) if exitcode and exitcode < 0 else str(exitcode)
            except (ValueError, TypeError):  # pragma: no cover
                signame = "Unknown"
            msg = (
                f"Subprocess {worker.process.pid} completed unexpectedly with "
                f"exitcode {exitcode} ({signame})"
            )
            logger.error(msg)
            fut.set_exception(ProcessError(msg))
        self._reap(worker)

    def _process_timeouts(self):
        now = time.monotonic()
        while self._deadlines and self._deadlines[0][0] <= now:
            _, _, worker, fut = heapq.heappop(self._deadlines)
            # A deadline is only live if this worker is still running that exact
            # task. Stale entries (task already finished, worker reused) are
            # ignored -- lazy deletion.
            if worker.fut is fut and worker.current is not None and not fut.done():
                logger.debug(f"Process {worker.process.pid} timed out, killing")
                kill_proc_tree(worker.process.pid, sig=signal.SIGKILL)
                fut.set_exception(TimeoutError("Task exceeded its timeout"))
                worker.current = None
                worker.fut = None
                self._reap(worker)

    def _retire_idle(self, worker: _Worker):
        # Ask an idle, reusable worker to shut down cleanly, then reap it.
        try:
            worker.conn.send(None)
        except (BrokenPipeError, OSError):  # pragma: no cover
            pass
        self._reap(worker)

    def _reap(self, worker: _Worker):
        if worker in self._workers:
            self._workers.remove(worker)
        p = worker.process
        try:
            worker.conn.close()
        except OSError:  # pragma: no cover
            pass
        if p.exitcode is None:
            # Still exiting; reap it when its sentinel fires so we never block
            # the manager loop on a slow finalizer.
            self._reaping[p.sentinel] = p
        else:
            p.join()

    def _busy(self) -> bool:
        return any(w.current is not None for w in self._workers)

    def _next_timeout(self) -> Optional[float]:
        while self._deadlines:
            deadline, _, worker, fut = self._deadlines[0]
            if worker.fut is fut and worker.current is not None and not fut.done():
                return max(0.0, deadline - time.monotonic())
            heapq.heappop(self._deadlines)  # discard stale head
        return None

    def _manage(self):
        while True:
            self._drain_pending()
            self._assign()

            if self.closed and not self._unassigned and not self._busy():
                # Drain: retire any idle reusable workers so we can exit.
                for w in list(self._workers):
                    if w.current is None:
                        self._retire_idle(w)

            if (
                self.closed
                and not self._unassigned
                and self._pending.empty()
                and not self._workers
                and not self._reaping
            ):
                break

            timeout = self._next_timeout()

            wait_list = [self._wake_r]
            conn_map = {}
            sentinel_map = {}
            for w in self._workers:
                conn_map[w.conn] = w
                sentinel_map[w.process.sentinel] = w
                wait_list.append(w.conn)
                wait_list.append(w.process.sentinel)
            for fd in self._reaping:
                wait_list.append(fd)

            ready = wait(wait_list, timeout)

            if self._wake_r in ready:
                try:
                    os.read(self._wake_r, 65536)
                except (BlockingIOError, OSError):  # pragma: no cover
                    pass

            handled = set()
            # Results first: a worker that finished may also have its sentinel
            # ready (it exits after the last task), and the result wins.
            for obj in ready:
                worker = conn_map.get(obj)
                if worker is not None:
                    handled.add(worker)
                    self._on_result(worker)

            # Unexpected deaths (OOM, external kill, crash).
            for obj in ready:
                worker = sentinel_map.get(obj) if isinstance(obj, int) else None
                if worker is None or worker in handled:
                    continue
                if worker in self._workers:
                    if worker.current is not None:
                        self._on_death(worker)
                    else:
                        self._reap(worker)  # idle worker died

            # Reap processes that have finished exiting.
            for obj in ready:
                if isinstance(obj, int) and obj in self._reaping:
                    p = self._reaping.pop(obj)
                    p.join()

            self._process_timeouts()

        os.close(self._wake_r)
        os.close(self._wake_w)


def _worker_loop(conn, initializer, initargs, finalizer, finalargs, max_tasks):
    """Runs inside the subprocess. Receives tasks over ``conn``, executes them,
    and sends back tagged results until it hits its task budget or receives a
    ``None`` shutdown sentinel."""
    if initializer:
        try:
            initializer(*initargs)
        except Exception:
            logger.exception("Initializer failed")

    count = 0
    try:
        while max_tasks is None or count < max_tasks:
            try:
                msg = conn.recv()
            except EOFError:
                break
            if msg is None:  # shutdown sentinel
                break

            fn, args, kwargs = msg
            try:
                result = fn(*args, **kwargs)
            except BaseException as e:
                payload = ("exc", e)
            else:
                payload = ("res", result)

            try:
                conn.send(payload)
            except Exception:  # e.g. result/exception not picklable
                logger.exception("Failed to send task result to pool")
            count += 1
    finally:
        if finalizer:
            try:
                finalizer(*finalargs)
            except Exception:
                logger.exception("Finalizer failed")
        conn.close()


# Taken from
# https://psutil.readthedocs.io/en/latest/index.html?highlight=children#kill-process-tree
def kill_proc_tree(pid, sig=signal.SIGTERM, include_parent=True,
                   timeout=None, on_terminate=None):  # pragma: no cover
    """Kill a process tree (including grandchildren) with signal
    "sig" and return a (gone, still_alive) tuple.
    "on_terminate", if specified, is a callback function which is
    called as soon as a child terminates.
    """
    if pid == os.getpid():
        raise ValueError("won't kill myself")

    parent = psutil.Process(pid)
    children = parent.children(recursive=True)
    if include_parent:
        children.append(parent)

    for p in children:
        try:
            p.send_signal(sig)
        except psutil.NoSuchProcess:
            pass

    gone, alive = psutil.wait_procs(children, timeout=timeout,
                                    callback=on_terminate)
    return (gone, alive)
