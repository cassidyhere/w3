import datetime
from enum import IntEnum
from typing import Optional, Union

from loguru import logger
from sqlalchemy import MetaData, Column, Integer, String, Table, DateTime
from sqlalchemy import select, and_, create_engine, func
from sqlalchemy.engine import Connection
from sqlalchemy.schema import CreateTable
from sqlalchemy.dialects import sqlite

from src.utils import (
    get_csv_last_timestamp,
    gen_datadir_csv,
    extract_symbol_from_file,
)


class MetaDataReflect(type):
    def __getattr__(self, table: str):
        try:
            return object.__getattribute__(self, table)
        except AttributeError:
            return self.metadata.tables[table]


class DB(metaclass=MetaDataReflect):
    metadata = MetaData()

    def __init__(self, url: str):
        self.engine = create_engine(url, echo=False)

    def connect(self) -> Connection:
        conn = self.engine.connect()
        return conn

    def create_all(self, drop: bool = False) -> None:
        if drop:
            self.metadata.drop_all(self.engine)
        self.metadata.create_all(self.engine, checkfirst=True)

    @classmethod
    def ddl(cls) -> str:
        sql = (
            str(CreateTable(cls.table).compile(dialect=sqlite.dialect()))
            .strip()
            .replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS")
            + ";\n"
        )
        return sql


db = DB("sqlite:///data.sql")


class DLogStatus(IntEnum):
    success = 1
    fail = 2
    not_found = 3


class DownloadLog(DB):
    table = Table(
        "download_log",
        DB.metadata,
        Column("symbol", String(16), primary_key=True),
        Column("interval", String(8), primary_key=True),
        Column("date", String(10), primary_key=True),
        Column(
            "status", Integer,
            nullable=False,
            comment="1-success,2-fail,3-not found"
        ),
        Column("last_timestamp", Integer, comment="下载文件最后一条记录的timestamp"),
        Column("insert_time", DateTime, default=datetime.datetime.now),
        Column("update_time", DateTime, onupdate=datetime.datetime.now),
    )

    @classmethod
    def count(cls) -> int:
        with db.connect() as conn:
            stmt = select(func.count(cls.table.c.symbol))
            return conn.execute(stmt).scalar() or 0

    @classmethod
    def list(cls) -> list:
        with db.connect() as conn:
            stmt = cls.table.select()
            return list(conn.execute(stmt))

    @classmethod
    def find_info(cls, conn: Connection, symbol: str, interval: str, date: str):
        t = cls.table
        stmt = select(t.c.status, t.c.last_timestamp).where(
            and_(
                t.c.symbol == symbol,
                t.c.interval == interval,
                t.c.date == date,
            )
        )
        return conn.execute(stmt).first()

    @classmethod
    def find_not_found_max_date(
        cls, conn: Connection, symbol: str, interval: str
    ) -> Union[None, str]:
        t = cls.table
        stmt = select(func.max(t.c.date)).where(
            and_(
                t.c.symbol == symbol,
                t.c.interval == interval,
                t.c.status == DLogStatus.not_found.value,
            )
        )
        return conn.execute(stmt).scalar()

    @classmethod
    def should_ignore(
        cls, conn: Connection, symbol: str, interval: str, date: str
    ) -> bool:
        info = cls.find_info(conn, symbol, interval, date)
        if info and info.status != DLogStatus.fail.value:
            return True
        max_date = cls.find_not_found_max_date(conn, symbol, interval)
        if max_date and date <= max_date:
            return True
        return False

    @classmethod
    def insert_or_update(
        cls,
        conn: Connection,
        symbol: str,
        interval: str,
        date: str,
        status: int,
        last_timestamp: Optional[int] = None,
    ) -> None:
        t = cls.table
        row = cls.find_info(conn, symbol=symbol, interval=interval, date=date)
        if row is not None:
            if row.status != status or row.last_timestamp != last_timestamp:
                stmt = (
                    t.update()
                    .where(
                        and_(
                            t.c.symbol == symbol,
                            t.c.interval == interval,
                            t.c.date == date,
                        )
                    )
                    .values(status=status, last_timestamp=last_timestamp)
                )
            else:
                return
        else:
            stmt = t.insert().values(
                symbol=symbol,
                interval=interval,
                date=date,
                status=status,
                last_timestamp=last_timestamp,
            )
        conn.execute(stmt)

    @classmethod
    def persist_ignore(cls, path: str = "../data/ignore") -> None:
        with open(path) as f:
            ignore = f.read().split("\n")
        with db.connect() as conn:
            for i in ignore:
                try:
                    symbol, interval, date = extract_symbol_from_file(i)
                except ValueError:
                    continue
                else:
                    cls.insert_or_update(
                        conn=conn,
                        symbol=symbol,
                        interval=interval,
                        date=date,
                        status=DLogStatus.not_found,
                    )
            conn.commit()

    @classmethod
    def init_from_csvs(cls, datadir: str = "../data") -> None:
        with db.connect() as conn:
            for path in gen_datadir_csv(datadir):
                logger.info(f"going to process {path}")
                symbol, interval, date = extract_symbol_from_file(path)
                timestamp = get_csv_last_timestamp(path)
                cls.insert_or_update(
                    conn=conn,
                    symbol=symbol,
                    interval=interval,
                    date=date,
                    status=DLogStatus.success,
                    last_timestamp=timestamp,
                )
            conn.commit()


if __name__ == "__main__":
    db.create_all(drop=True)
    DownloadLog.init_from_csvs()
