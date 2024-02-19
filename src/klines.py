"""策略：


"""

from decimal import Decimal
from typing import Union, List


class KLine:
    def __init__(self, data: List):
        self.open_time: int = data[0]  # k线开盘时间
        self.open = Decimal(data[1])  # 开盘价
        self.high = Decimal(data[2])  # 最高价
        self.low = Decimal(data[3])  # 最低价
        self.close = Decimal(data[4])  # 收盘价(当前K线未结束的即为最新价)
        self.volume = Decimal(data[5])  # 成交量
        self.close_time: int = data[6]  # k线收盘时间
        self.quote_volume = Decimal(data[7])  # 成交额
        self.count = data[8]  # 成交笔数
        self.taker_buy_volume = Decimal(data[9])  # 主动买入成交量
        self.taker_buy_quote_volume = Decimal(data[10])  # 主动买入成交额
        self.ignore = data[11]  # 请忽略该参数

    @property
    def incr(self) -> Decimal:
        """涨幅"""
        return (self.close - self.open) / self.close

    def is_incr_gt(self, n: float) -> bool:
        return self.incr > Decimal(n)


if __name__ == "__main__":
    i = [
        1499040000000,  # k线开盘时间
        "0.01634790",  # 开盘价
        "0.80000000",  # 最高价
        "0.01575800",  # 最低价
        "0.01577100",  # 收盘价(当前K线未结束的即为最新价)
        "148976.11427815",  # 成交量
        1499644799999,  # k线收盘时间
        "2434.19055334",  # 成交额
        308,  # 成交笔数
        "1756.87402397",  # 主动买入成交量
        "28.46694368",  # 主动买入成交额
        "17928899.62484339",  # 请忽略该参数
    ]
    kline = KLine(i)
    print(kline.incr)
    print(kline.is_incr_gt(0.05))
