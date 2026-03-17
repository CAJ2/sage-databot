import builtins
import time


def cfg_log() -> None:
    """Patch print() to prefix each line with elapsed time since this call."""
    start = time.monotonic()
    original_print = builtins.print

    def _timed_print(*args, **kwargs):
        delta = time.monotonic() - start
        original_print(f"[+{delta:.1f}s]", *args, **kwargs)

    builtins.print = _timed_print
