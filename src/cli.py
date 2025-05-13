from prefect import Flow
import argparse
from dotenv import load_dotenv


def setup_cli(flow: Flow, setup_parser=None):
    load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--deploy",
        action="store_true",
        default=False,
        help="Deploy the flow to Prefect",
    )
    if setup_parser is not None:
        setup_parser(parser)
    args = parser.parse_args()
    flow(args)
