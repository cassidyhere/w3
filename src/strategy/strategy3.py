"""策略1：最近的5根1小时k线，至少4根涨了且最近2根是涨的"""

from decimal import Decimal
from typing import Union

from src.strategy.executor import Klines, StrategyPipeline, run_executor
from src.strategy.calc import calc_kl_incr
from src.utils import log2file

log2file("strategy3.log")


def has_4_incr(klines: Klines) -> Union[None, bool]:
    incr = calc_kl_incr(klines)
    n = 0
    for i in incr:
        if i > Decimal(0):
            n += 1
    if n >= 4 and incr[-1] > 0 and incr[-2] > 0:
        return True
    return False


def main():
    strategy = StrategyPipeline([has_4_incr])
    run_executor(strategy, interval="1h")


if __name__ == "__main__":
    main()
