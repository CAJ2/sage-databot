# requirements: project
import time

import wmill


def main(
    script_path: str,
    row: dict[str, object],
    item_index: int,
    batch_size: int,
    delay_between_batches_s: float,
    static_args: dict[str, object] | None = None,
):
    if delay_between_batches_s > 0 and batch_size > 0:
        batch_index = item_index // batch_size
        sleep_time = batch_index * delay_between_batches_s
        if sleep_time > 0:
            print(
                f"Item {item_index} (batch {batch_index}): sleeping {sleep_time:.1f}s"
            )
            time.sleep(sleep_time)
    args = {**(static_args or {}), **row}
    print(f"Running {script_path} with args: {list(args.keys())}")
    return wmill.run_script_by_path(script_path, args=args)
