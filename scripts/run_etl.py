#!/usr/bin/env python
"""
Orchestrate ETL: Bronze -> Silver -> Gold

Usage:
  poetry run python scripts/run_etl.py [--bronze] [--silver] [--gold]
If no stage flags are provided, all stages run.
"""
from __future__ import annotations

import argparse
import sys
from typing import List

from core.logging import get_logger
from core.config import settings

# Stage entrypoints
from etl.bronze.ingest_bronze import main as run_bronze
from etl.silver.clean_silver import main as run_silver
from etl.gold.build_gold import main as run_gold


logger = get_logger(__name__)


def parse_args(argv: List[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run ETL pipeline stages")
    p.add_argument("--bronze", action="store_true", help="Run Bronze stage only")
    p.add_argument("--silver", action="store_true", help="Run Silver stage only")
    p.add_argument("--gold", action="store_true", help="Run Gold stage only")
    return p.parse_args(argv)


def main(argv: List[str] | None = None) -> None:
    argv = argv if argv is not None else sys.argv[1:]
    args = parse_args(argv)

    selected = any([args.bronze, args.silver, args.gold])

    logger.info(
        "Starting ETL orchestration",
        extra={
            "env": settings.environment,
            "selected": {
                "bronze": args.bronze,
                "silver": args.silver,
                "gold": args.gold,
            },
        },
    )

    if args.bronze or not selected:
        logger.info("Running Bronze stage")
        run_bronze()

    if args.silver or not selected:
        logger.info("Running Silver stage")
        run_silver()

    if args.gold or not selected:
        logger.info("Running Gold stage")
        run_gold()

    logger.info("ETL orchestration completed")


if __name__ == "__main__":
    main()
