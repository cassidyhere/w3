from binance.spot import Spot


def make_spot_clint(base_url: str = "https://api3.binance.com") -> Spot:
    proxies = {"https": "http://127.0.0.1:7890"}
    client = Spot(proxies=proxies, base_url=base_url)
    return client
