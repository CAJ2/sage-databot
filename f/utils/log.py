import builtins
import time

import wmill


class BatchProgress:
    total: int
    processed: int

    def __init__(self, total: int) -> None:
        self.total = total
        self.processed = 0

    def update(self, last_cursor: object) -> None:
        self.processed += 1
        wmill.set_state({"processed": self.processed, "last_cursor": str(last_cursor)})
        if self.total > 0:
            wmill.set_progress(int(self.processed / self.total * 99))


def cfg_log() -> None:
    """Patch print() to prefix each line with elapsed time since this call."""
    start = time.monotonic()
    original_print = builtins.print

    def _timed_print(
        *args: object,
        sep: str | None = " ",
        end: str | None = "\n",
        flush: bool = False,
    ) -> None:
        delta = time.monotonic() - start
        original_print(f"[+{delta:.1f}s]", *args, sep=sep, end=end, flush=flush)

    builtins.print = _timed_print
