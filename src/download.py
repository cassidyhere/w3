import os
import time
import datetime
from functools import cached_property
from typing import Union, Optional

import pandas as pd
from loguru import logger
from binance.spot import Spot
from sqlalchemy.engine import Connection

from src.client import make_spot_clint
from src.exchange import Exchange
from src.sql import db, DownloadLog, DLogStatus
from src.utils import datetime2timestamp, datetime2str, date_range, df2csv


class SymbolDownloadHelper:
    def __init__(
        self,
        downloader: "SpotDownloader",
        symbol: str,
        interval: str,
        date: Union[str, datetime.datetime],
    ):
        self.downloader = downloader
        self.symbol = symbol
        self.interval = interval
        self.date = datetime2str(date)
        self._df = None

    @property
    def name(self) -> str:
        return f"{self.symbol}-{self.interval}-{self.date}"

    @property
    def filename(self) -> str:
        return self.name + ".csv"

    @property
    def path(self) -> str:
        return os.path.join(
            self.downloader.datadir, self.symbol, self.interval, self.filename
        )

    def should_ignore(self) -> bool:
        args = (self.symbol, self.interval, self.date)
        if self.downloader.ignore.should_ignore(*args):
            return True
        if DownloadLog.should_ignore(self.downloader.conn, *args):
            self.downloader.ignore.add(*args)
            return True
        return False

    def download_klines(
        self, start_time: datetime.datetime, end_time: datetime.datetime
    ) -> None:
        data = self.downloader.client.klines(
            self.symbol,
            interval=self.interval,
            startTime=datetime2timestamp(start_time),
            endTime=datetime2timestamp(end_time),
        )
        df = pd.DataFrame(
            data,
            columns=[
                "open_time",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "close_time",
                "quote_volume",
                "count",
                "taker_buy_volume",
                "taker_buy_quote_volume",
                "ignore",
            ],
        )
        self._df = df

    @property
    def df(self) -> pd.DataFrame:
        if self._df is None:
            raise ValueError("df nof found")
        return self._df

    @property
    def status(self) -> int:
        if self.df.empty:
            return DLogStatus.not_found.value
        return DLogStatus.success.value

    @property
    def max_timestamp(self) -> Union[None, int]:
        if not self.df.empty:
            return self.df.iloc[-1]["open_time"]

    def persist(self) -> None:
        if self.df.empty:
            logger.info(f"{self.name} data not found")
        else:
            df2csv(self.df, self.path)
        DownloadLog.insert_or_update(
            conn=self.downloader.conn,
            symbol=self.symbol,
            interval=self.interval,
            date=self.date,
            status=self.status,
            last_timestamp=self.max_timestamp,
        )


class IgnoreDict(dict):
    def should_ignore(self, symbol: str, interval: str, date: str) -> bool:
        current = self.get((symbol, interval))
        if current:
            if date > current:
                self.add(symbol, interval, date)
            return True
        return False

    def add(self, symbol: str, interval: str, date: str) -> None:
        self[(symbol, interval)] = date


class SpotDownloader:
    """现货下载器"""

    def __init__(self, datadir: str = "../data", conn: Optional[Connection] = None):
        os.makedirs(datadir, exist_ok=True)
        self.datadir = datadir
        self.conn = conn or db.connect()
        self.ignore = IgnoreDict()

    @cached_property
    def client(self) -> Spot:
        return make_spot_clint()

    def download_his_klines(
        self,
        symbol: str,
        interval: str,
        start_time: datetime.datetime,
        end_time: datetime.datetime,
        ignore_exists: bool = True,
    ) -> None:
        """下载k线数据"""

        helper = SymbolDownloadHelper(self, symbol, interval, start_time)

        if ignore_exists and helper.should_ignore():
            logger.info(f"ignore {helper.name}")
            return

        for i in (2, 4, 8, 16):
            try:
                helper.download_klines(start_time, end_time)
            except Exception as e:
                logger.warning(f"download klines failed: {e}")
                time.sleep(i)
            else:
                helper.persist()
                break

    def download_ndays_klines(
        self,
        symbol: str,
        interval: str,
        start_date: Union[str, datetime.datetime],
        ndays: Optional[int] = None,
        ignore_exists: bool = True,
    ) -> None:
        """下载多天的k线数据"""
        today = datetime.date.today()

        for i in date_range(start_date, ndays=ndays)[::-1]:
            if i.date() > today:
                break
            self.download_his_klines(
                symbol=symbol,
                interval=interval,
                start_time=i,
                end_time=i + datetime.timedelta(days=1),
                ignore_exists=ignore_exists,
            )

    def close(self) -> None:
        self.conn.commit()
        self.conn.close()


def download_usdt_symbols_klines(
    interval: str,
    start_date: Union[str, datetime.datetime],
    ndays: Optional[int] = None,
    symbols: Optional[list[str]] = None,
) -> None:
    if not symbols:
        exchange = Exchange.from_json()
        symbols = exchange.get_symbols({"quoteAsset": "USDT"})

    downloader = SpotDownloader()
    try:
        for s in symbols:
            downloader.download_ndays_klines(
                s, interval=interval, start_date=start_date, ndays=ndays
            )
    finally:
        downloader.close()


if __name__ == "__main__":
    download_usdt_symbols_klines("15m", start_date="20240101")
    download_usdt_symbols_klines("5m", start_date="20240101")
    download_usdt_symbols_klines("3m", start_date="20240101")
    download_usdt_symbols_klines("1m", start_date="20240101")
    download_usdt_symbols_klines("30m", start_date="20240101")
    download_usdt_symbols_klines("2h", start_date="20240101")
    download_usdt_symbols_klines("4h", start_date="20240101")
