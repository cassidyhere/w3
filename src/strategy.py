import datetime
from decimal import Decimal
from dataclasses import dataclass
from functools import cached_property
from typing import Dict, Iterable, List, Callable, Any, Literal, Union, Optional

from src.client import make_spot_clint
from src.exchange import Exchange


@dataclass
class KlineItem:
    symbol: str
    interval: str
    open: str
    close: str
    high: str
    low: str
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


class KlinesManager:
    def __init__(self):
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


def binance_timestamp2dt(ts: int) -> datetime.datetime:
    return datetime.datetime.fromtimestamp(ts / 1000)


def interval2timedelta(interval: str) -> datetime.timedelta:
    n, t = int(interval[:-1]), interval[-1].lower()
    if t == "h":
        kw = dict(hours=n)
    elif t == "m":
        kw = dict(minutes=n)
    else:
        raise ValueError(f"Invalid interval: {interval}")
    return datetime.timedelta(**kw)


class BinanceSpotDownloader:
    def __init__(self):
        self.client = make_spot_clint()

    def download_klines(self, symbol: str, interval: str, limit: int) -> List[KlineItem]:
        data: List[List] = self.client.klines(symbol, interval=interval, limit=limit)
        klines = []
        for i in data:
            klines.append(KlineItem(
                symbol=symbol,
                interval=interval,
                open=i[1],
                high=i[2],
                low=i[3],
                close=i[4],
                dt=binance_timestamp2dt(i[0]),
            ))
        return klines


def is_1h_incr_gt_5p(klines: Klines) -> bool:
    a, b = Decimal(klines[-1].close), Decimal(klines[-2].close)
    return (a - b) / b >= Decimal(0.05)


class StrategyPipeline:
    def __init__(self, strategies: List[Callable[[Any], bool]]):
        self.strategies = strategies

    def __call__(self, data: Any) -> bool:
        return any(f(data) for f in self.strategies)


class OrderParams:
    pass


class OrderProxy:
    def make_new_order(self, params: OrderParams):
        raise NotImplementedError

    def cancel_order(self, order_id: int):
        raise NotImplementedError


@dataclass
class BinanceOrderParams(OrderParams):
    symbol: str
    side: Literal["SELL", "BUY"]
    type: Literal["MARKET"]
    quantity: Decimal
    price: Decimal
    timestamp: int


class BinanceOrderProxy(OrderProxy):
    def make_new_order(self, params: BinanceOrderParams):
        pass

    def cancel_order(self, order_id: int):
        pass


class Executor:
    def __init__(self, strategy: StrategyPipeline):
        self.strategy = strategy
        self.downloader = BinanceSpotDownloader()
        self.klines_manager = KlinesManager()
        self.init()

    @cached_property
    def symbols(self) -> List[str]:
        exchange = Exchange.from_client()
        symbols = exchange.get_symbols({"quoteAsset": "USDT"})
        return symbols

    def init(self):
        for s in self.symbols:
            self.downloader.download_klines(s, interval="1h", limit=5)

    def run_forever(self):
        pass


if __name__ == "__main__":
    from pprint import pprint
    dl = Downloader()
    data = dl.download_klines("ETHUSDT", "1h", 2)
    for i in data:
        print(datetime.datetime.fromtimestamp(i[0] / 1000))
        print(i)
