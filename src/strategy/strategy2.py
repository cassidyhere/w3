from typing import Union

import pandas as pd
from loguru import logger

from src.strategy.executor import Klines, StrategyPipeline, run_executor
from src.utils import log2file

log2file("strategy2.log")


def has_incr_gt_5p(klines: Klines) -> Union[None, bool]:
    df = klines.to_df()
    df["gt5p"] = df["incr"] >= 0.05

    with pd.option_context("display.max_columns", None):
        logger.info(f"klines df:\n{df}")

    return df["gt5p"].any()


def main():
    strategy = StrategyPipeline([has_incr_gt_5p])
    run_executor(strategy, interval="1h", init_limit=10)


if __name__ == "__main__":
    main()
