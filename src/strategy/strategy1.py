"""策略1：1小时k线涨幅大于5%"""

from decimal import Decimal
from typing import Union

from src.strategy.executor import Klines, StrategyPipeline, run_executor
from src.strategy.calc import calc_kl_last_incr
from src.utils import log2file

log2file("strategy1.log")


def is_kl_last_incr_gt_5p(klines: Klines) -> Union[None, bool]:
    incr = calc_kl_last_incr(klines)
    if incr:
        return incr >= Decimal(0.05)


def main():
    strategy = StrategyPipeline([is_kl_last_incr_gt_5p])
    run_executor(strategy, interval="1h")


if __name__ == "__main__":
    main()
