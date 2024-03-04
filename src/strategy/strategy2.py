from decimal import Decimal
from typing import Union

from src.strategy.executor import Klines, StrategyPipeline, run_executor
from src.strategy.calc import calc_kl_incr


def has_4_incr(klines: Klines) -> Union[None, bool]:
    n = 0
    for i in calc_kl_incr(klines):
        if i > Decimal(0):
            n += 1
    return n >= 4


def main():
    strategy = StrategyPipeline([has_4_incr])
    run_executor(strategy, interval="5m")


if __name__ == "__main__":
    main()
