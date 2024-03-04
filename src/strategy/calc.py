from decimal import Decimal
from typing import Union, List

from src.strategy.kline import Klines


t_num = Union[str, float, Decimal]


def calc_incr(a: t_num, b: t_num) -> Decimal:
    return (Decimal(a) - Decimal(b)) / Decimal(b)


def calc_kl_last_incr(klines: Klines) -> Union[None, Decimal]:
    if len(klines) < 2:
        return
    return calc_incr(klines[-2].close, klines[-1].close)


def calc_kl_incr(klines: Klines) -> List[Decimal]:
    if len(klines) < 2:
        return []
    incr = []
    for i in range(len(klines) - 1):
        incr.append(calc_incr(klines[i + 1].close, klines[i].close))
    return incr
