"""``python -m pyraft_lab.api`` - run the API server directly.

The ordinary entry point is ``pyraft-lab serve``; this exists so the app can be started
without the CLI in the way, which is what an autoreloading dev loop wants.
"""

from __future__ import annotations

import argparse
from pathlib import Path


def main() -> None:
    import uvicorn

    parser = argparse.ArgumentParser(description="Serve the PyRaft Lab API.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument(
        "--workspace",
        type=Path,
        default=Path("."),
        help="Where cluster.yaml, scenarios/ and results/ live.",
    )
    args = parser.parse_args()

    from pyraft_lab.api.server import create_app

    uvicorn.run(create_app(args.workspace), host=args.host, port=args.port, log_level="info")


if __name__ == "__main__":
    main()
