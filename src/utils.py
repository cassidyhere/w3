import os
import datetime
from typing import Union, List, Iterator, Tuple, Optional

import pandas as pd
from loguru import logger


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
