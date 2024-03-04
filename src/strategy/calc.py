from decimal import Decimal
from typing import Union, List

from src.strategy.kline import KlineItem, Klines


t_num = Union[str, float, Decimal]


def calc_incr(kline: KlineItem) -> Decimal:
    return (Decimal(kline.close) - Decimal(kline.open)) / Decimal(kline.open)


def calc_kl_last_incr(klines: Klines) -> Union[None, Decimal]:
    if klines:
        return calc_incr(klines[-1])


def calc_kl_incr(klines: Klines) -> List[Decimal]:
    incr = list(map(calc_incr, klines))
    return incr
