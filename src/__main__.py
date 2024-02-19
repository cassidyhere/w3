from binance.spot import Spot as Client

proxies = {"https": "http://127.0.0.1:7890"}
client = Client(proxies=proxies)

# Get server timestamp
print(client.time())
# Get klines of BTCUSDT at 1m interval
data = client.klines("BTCUSDT", "1m")
print(len(data))
print(data[-2])
print(data[-1])
data = client.klines("BTCUSDT", "5m")
print(len(data))
print(data[0])
print(data[-1])
# Get last 10 klines of BNBUSDT at 1h interval
# print(client.klines("BNBUSDT", "1h", limit=10))
