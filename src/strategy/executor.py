import time
import datetime
from decimal import Decimal
from dataclasses import dataclass
from functools import cached_property
from typing import List, Callable, Any, Literal

from loguru import logger

from src.exchange import Exchange
from src.strategy.kline import Klines, KlinesManager
from src.strategy.download import BinanceSpotDownloader
from src.strategy.calc import calc_incr
from src.utils import get_next_runtime


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
    def __init__(
        self, strategy: StrategyPipeline, interval: str, init_limit: int = 5
    ):
        logger.info("strategy executor started")

        self.strategy = strategy
        self.interval = interval

        self.klines_manager = KlinesManager(BinanceSpotDownloader())
        self.exec_strategy(init_limit)

    @cached_property
    def symbols(self) -> List[str]:
        exchange = Exchange.from_json()
        symbols = exchange.get_symbols({"quoteAsset": "USDT"})
        logger.info(f"got {len(symbols)} symbols: {str(symbols)[:100]}...")
        return symbols

    def exec_strategy(self, limit: int) -> None:
        for s in self.symbols:
            try:
                self.klines_manager.download_klines(
                    s, interval=self.interval, limit=limit
                )
            except Exception as e:
                logger.info(f"download {s} failed: {e}")
                continue

            klines = self.klines_manager.get(f"{s}{self.interval}")
            incr = round(calc_incr(klines[-1]), 4)
            logger.info(f"download {s} done, got {len(klines)} klines, "
                        f"last kline: {klines[-1]}, last incr: {incr}")

            if not klines:
                continue
            if self.strategy(klines):
                logger.info(f"{s} pass strategy, klines: {klines[-5:]}")

    def get_next_runtime(self) -> datetime.datetime:
        return get_next_runtime(self.interval)

    def run_forever(self, seconds: int = 10) -> None:
        logger.info("strategy looper started")

        next_runtime = self.get_next_runtime()
        while 1:
            if datetime.datetime.now() < next_runtime:
                time.sleep(seconds)
                continue

            logger.info(f"runtime: {next_runtime}")
            self.exec_strategy(1)
            next_runtime = self.get_next_runtime()


def run_executor(
    strategy: StrategyPipeline, interval: str, init_limit: int = 5
) -> None:
    executor = Executor(strategy, interval=interval, init_limit=init_limit)
    executor.run_forever()
