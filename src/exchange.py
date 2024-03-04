import os
import json
from typing import Dict, Any, List

from src.client import make_spot_clint


class Exchange:
    def __init__(self, data: Dict):
        self.data = data

    @classmethod
    def from_json(cls) -> "Exchange":
        file = os.path.join(
            os.path.abspath(os.path.dirname(__file__)), "exchange_info.json"
        )
        with open(file) as f:
            data = json.load(f)
        return cls(data)

    @classmethod
    def from_client(cls) -> "Exchange":
        client = make_spot_clint()
        data = client.exchange_info()
        return cls(data)

    def to_json(self) -> None:
        with open("exchange_info.json", "w") as f:
            json.dump(self.data, f)

    def get_symbols(self, filters: Dict[str, Any]) -> List[str]:
        symbols = []
        for s in self.data["symbols"]:
            flag = 1
            for k, v in filters.items():
                if s[k] != v:
                    flag = 0
                    break
            if flag == 0:
                continue
            symbols.append(s["symbol"])
        symbols.sort()
        return symbols


if __name__ == "__main__":
    exchange = Exchange.from_client()
    exchange.to_json()
