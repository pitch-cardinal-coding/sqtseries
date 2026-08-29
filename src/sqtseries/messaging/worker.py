"""Worker pool: N asyncio workers consuming from ingress and serving queries.
Each worker runs its own loop iteration over ingress + broker. zmq sockets are
not thread-shared; asyncio tasks *are* safe on the same socket only via the
mutex-free event loop — so this pool lives on a single loop with cooperative
workers. For multiple processes use ProcessPool + per-process context.
"""

import asyncio
from collections.abc import Awaitable, Callable

import structlog

log = structlog.get_logger(__name__)


class WorkerPool:
    """Run N asyncio worker tasks, each polling the ingress/broker loops.

    ``step`` is an async callable that performs one unit of work (e.g. one

    drain iteration) and returns True if it did any work. The pool starts

    ``size`` tasks; on stop it cancels the workers and awaits their completion.

    """

    def __init__(self, step: Callable[[], Awaitable[bool]], *, size: int = 1):
        if size < 1:
            raise ValueError("worker pool size must be >= 1")
        self._step = step
        self.size = size
        self._tasks: list[asyncio.Task] = []
        self._running = False

    async def start(self) -> None:
        self._running = True
        self._tasks = [
            asyncio.create_task(self._run(i), name=f"worker-{i}")
            for i in range(self.size)
        ]

    async def _run(self, idx: int) -> None:
        try:
            while self._running:
                try:
                    did_work = await self._step()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.exception("worker %d error", idx)
                    await asyncio.sleep(0.05)
                    continue
                # Yield to the loop: recv(flags=NOBLOCK) raising zmq.Again
                # does NOT release the event loop, so a tight step() loop would
                # starve timers and other tasks (verified 2026-08-07: 112k
                # spins/sec, call_later never fired). When the step found NO
                # work, sleep 10ms instead of 0: an idle loop that re-grabs the
                # GIL continuously starves CPU-bound worker threads (e.g. a
                # query offloaded via asyncio.to_thread), so long HTTP/ZMQ
                # queries stalled under idle load — and it burns CPU. 10ms only
                # delays the FIRST message after an idle period; once work is
                # flowing, busy cycles use sleep(0) with no added latency.

                await asyncio.sleep(0 if did_work else 0.01)
        except asyncio.CancelledError:
            pass

    async def stop(self) -> None:
        self._running = False
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []

    @property
    def running(self) -> bool:
        return self._running
