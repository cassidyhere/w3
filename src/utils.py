import os
import datetime
from typing import Union, List, Iterator, Tuple, Optional

import pandas as pd
from loguru import logger


def binance_timestamp2dt(ts: int) -> datetime.datetime:
    return datetime.datetime.fromtimestamp(ts / 1000)


def split_interval(interval: str) -> Tuple[int, str]:
    return int(interval[:-1]), interval[-1].lower()


def interval2timedelta(interval: str) -> datetime.timedelta:
    n, t = split_interval(interval)
    if t == "h":
        kw = dict(hours=n)
    elif t == "m":
        kw = dict(minutes=n)
    else:
        raise ValueError(f"Invalid interval: {interval}")
    return datetime.timedelta(**kw)


def get_next_runtime(interval: str) -> datetime.datetime:
    n, t = split_interval(interval)
    rt = datetime.datetime.now().replace(second=0, microsecond=0)
    if t == "h":
        rt = rt.replace(minute=0) + datetime.timedelta(hours=n)
    elif t == "m":
        rt = rt + datetime.timedelta(minutes=n)
    else:
        raise ValueError(f"Invalid interval: {interval}")
    return rt


def datetime2timestamp(dt: Union[int, datetime.datetime]) -> int:
    if isinstance(dt, int):
        return dt
    return int(dt.timestamp() * 1000)


def datetime2str(dt: Union[str, datetime.datetime], fmt: str = "%Y-%m-%d") -> str:
    if isinstance(dt, datetime.datetime):
        return dt.strftime(fmt)
    return dt


def date_range(
    date: Union[str, datetime.datetime], ndays: Optional[int] = None
) -> List[datetime.datetime]:
    if isinstance(date, str):
        date = pd.to_datetime(date).to_pydatetime()
    end = None if ndays else datetime.date.today()
    return pd.date_range(start=date, end=end, periods=ndays, freq="d").tolist()


def remove_trailing_0s(s: str) -> str:
    if not s:
        return s
    index = len(s) - 1
    while index >= 0 and s[index] == "0":
        index -= 1
    if index == -1:
        return ""
    return s[:index + 1]


def df2csv(df: pd.DataFrame, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)
    logger.info(f"save {os.path.basename(path)} done")


def get_csv_last_timestamp(path: str) -> int:
    df = pd.read_csv(path)
    last_row = df.iloc[-1]
    return last_row["open_time"]


def gen_datadir_csv(datadir: str = "../data") -> Iterator[str]:
    for s in os.listdir(datadir):
        dir1 = os.path.join(datadir, s)
        if not os.path.isdir(dir1):
            continue
        for i in os.listdir(dir1):
            dir2 = os.path.join(dir1, i)
            if not os.path.isdir(dir2):
                continue
            for f in os.listdir(dir2):
                path = os.path.join(dir2, f)
                if not path.endswith("csv"):
                    continue
                yield path


def extract_symbol_from_file(file: str) -> Tuple[str, str, str]:
    file = os.path.basename(file)
    symbol, interval, date = file.replace(".csv", "").split("-", 2)
    return symbol, interval, date


def log2file(file: str) -> None:
    log_dir = os.path.join(
        os.path.abspath(os.path.dirname(os.path.dirname(__file__))), "log"
    )
    os.makedirs(log_dir, exist_ok=True)
    logger.add(os.path.join(os.getenv("LOG_DIR", log_dir), file), enqueue=True)
