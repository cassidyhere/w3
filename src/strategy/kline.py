import datetime
from dataclasses import dataclass
from typing import Dict, Iterable, List, Union, Optional

import pandas as pd

from src.utils import interval2timedelta, remove_trailing_0s


@dataclass
class KlineItem:
    symbol: str
    interval: str
    open: str
    high: str
    low: str
    close: str
    dt: datetime.datetime

    @property
    def name(self) -> str:
        return self.symbol + self.interval

    def is_valid_next_item(self, kline: "KlineItem") -> bool:
        return (
            kline.symbol == self.symbol
            and kline.interval == self.interval
            and kline.dt - self.dt == interval2timedelta(self.interval)
        )

    def __repr__(self) -> str:
        s_args = ",".join((
            self.symbol,
            self.interval,
            remove_trailing_0s(self.open),
            remove_trailing_0s(self.high),
            remove_trailing_0s(self.low),
            remove_trailing_0s(self.close),
            str(self.dt),
        ))
        return f"{self.__class__.__name__}({s_args})"


class Klines(list):
    def __init__(self, klines: Optional[Iterable[KlineItem]] = None):
        super().__init__(list(klines or []))

    @property
    def symbol(self) -> str:
        return self[0].symbol

    @property
    def interval(self) -> str:
        return self[0].interval

    @property
    def name(self) -> str:
        return self[0].name

    def append(self, kline: KlineItem) -> None:
        if self:
            assert self[-1].is_valid_next_item(kline)
        super().append(kline)

    def extend(self, klines: Iterable[KlineItem]) -> None:
        for i in sorted(klines, key=lambda x: x.dt):
            self.append(i)

    def to_df(self) -> pd.DataFrame:
        df = pd.DataFrame(self).astype({"close": "float", "open": "float"})
        df["incr"] = (df["close"] - df["open"]) / df["open"]
        return df


class KlinesManager:
    def __init__(self, downloader: "SpotDownloader"):
        self.downloader = downloader
        self.klines_dict: Dict[str, Klines] = {}

    def get(self, name: str) -> Union[None, Klines]:
        return self.klines_dict.get(name)

    def add(self, data: Union[KlineItem, List[KlineItem]]) -> None:
        if isinstance(data, KlineItem):
            data = [data]

        if not data:
            return

        klines = self.klines_dict.setdefault(data[0].name, Klines())
        klines.extend(data)

    def download_klines(self, symbol: str, interval: str, limit: int) -> None:
        data = self.downloader.download_klines(symbol, interval=interval, limit=limit)
        self.add(data)


def get_klines_df(symbol: str, interval: str, limit: int) -> pd.DataFrame:
    from src.strategy.download import BinanceSpotDownloader

    proxy = KlinesManager(BinanceSpotDownloader())
    proxy.download_klines(symbol, interval=interval, limit=limit)
    klines = proxy.get(symbol + interval)
    return klines.to_df()


if __name__ == "__main__":
    with pd.option_context("display.max_columns", None):
        print(get_klines_df("BTTCUSDT", "1h", 5))
