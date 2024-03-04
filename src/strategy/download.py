from typing import List

from src.client import make_spot_clint
from src.utils import binance_timestamp2dt

from src.strategy.kline import KlineItem


class SpotDownloader:
    def download_klines(
        self, symbol: str, interval: str, limit: int
    ) -> List[KlineItem]:
        raise NotImplementedError


class BinanceSpotDownloader(SpotDownloader):
    def __init__(self):
        self.client = make_spot_clint()

    def download_klines(
        self, symbol: str, interval: str, limit: int
    ) -> List[KlineItem]:
        data: List[List] = self.client.klines(symbol, interval=interval, limit=limit)
        klines = []
        for i in data:
            klines.append(
                KlineItem(
                    symbol=symbol,
                    interval=interval,
                    open=i[1],
                    high=i[2],
                    low=i[3],
                    close=i[4],
                    dt=binance_timestamp2dt(i[0]),
                )
            )
        return klines
