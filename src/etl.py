import os
from typing import Optional, Union

import pandas as pd
from src.utils import extract_symbol_from_file


DataLimit = tuple[str, str]


def timestamp2dt_ps(ps: pd.Series) -> pd.Series:
    ps = pd.to_datetime(ps, unit="ms", utc=True).dt.tz_convert("Asia/Shanghai")
    return ps


def merge_his_klines(
    datadir: str, date_limit: Optional[DataLimit] = None
) -> Union[None, pd.DataFrame]:
    """合并datadir目录下的csv文件为DataFrame，并根据open_time去重"""

    dfs = []
    for f in os.listdir(datadir):
        if date_limit:
            *_, date = extract_symbol_from_file(f)
            if date < date_limit[0] or date > date_limit[1]:
                continue
        item = pd.read_csv(os.path.join(datadir, f))
        dfs.append(item)

    if not dfs:
        return
    df = pd.concat(dfs)
    df = df.drop_duplicates(subset=["open_time"]).reset_index(drop=True)
    df["open_date"] = timestamp2dt_ps(df["open_time"])
    return df


def filter_incr_gt(df: pd.DataFrame, n: float) -> pd.DataFrame:
    """过滤df收益率大于n的记录"""

    df["incr"] = (df["close"] - df["open"]) / df["open"]
    df["next_incr"] = df["incr"].shift(1)
    return df[df["incr"] > n]


def filter_draw_down_lt(df: pd.DataFrame, n: float) -> pd.DataFrame:
    """过滤df回撤小于n的记录"""

    draw_down = (df["close"] - df["high"]) / df["high"]
    return df[draw_down < n]


def calc_cum_return(
    datadir: str = "../data",
    interval: str = "5m",
    n: float = 0.05,
    loss: float = 0.002,
    date_limit: Optional[DataLimit] = None,
) -> float:
    """计算累计收益率

    策略：当k线涨幅大于n时买入，interval后卖出

    loss: 手续费
    """

    def gen_symbol_dir():
        for symbol in os.listdir(datadir):
            dir_ = os.path.join(datadir, symbol, interval)
            if os.path.exists(dir_) and os.path.isdir(dir_):
                yield dir_

    cum_return = 1.0
    for p in gen_symbol_dir():
        df = merge_his_klines(p, date_limit=date_limit)
        if df is None:
            continue
        df = filter_incr_gt(df, n)
        # df = filter_draw_down_lt(df, 0.1)
        # df = df[df["next_incr"] > 0]
        # df = df[df["close"] == df["high"]]
        if df.empty:
            continue
        print(df)
        cum_return *= (df["next_incr"] + 1).prod() * (1 - loss) ** df.shape[0]
        # print(f"calc {p} done, cum return: {cum_return}")
    return cum_return


if __name__ == "__main__":
    with pd.option_context("display.max_columns", None):
        logs = []
        for i in (
            ("2024-01-01", "2024-01-31"),
            ("2024-02-01", "2024-02-28"),
        ):
            ret = calc_cum_return(date_limit=i, loss=0.002, interval="1m")
            logs.append(f"{i[0]}~{i[1]}: {ret}")
        for i in logs:
            print(i)
