import json
import websocket
from confluent_kafka import Producer
from src.common.params import *

kafka_config = {
    "bootstrap.servers": bootstrap_server,
    "client.id": "finnhub-producer"
}
producer = Producer(kafka_config)


def on_message(ws, message):
    json_message = json.loads(message)
    message_data = json_message["data"]
    for transaction in message_data:
        if "data" not in transaction:
            print(transaction)
            producer.produce(kafka_topic, json.dumps(transaction))


def on_error(ws, error):
    print(error)


def on_open(ws):
    print("opened")
    # ws.send('{"type":"subscribe","symbol":"AAPL"}')
    # ws.send('{"type":"subscribe","symbol":"AMZN"}')
    ws.send('{"type":"subscribe","symbol":"BINANCE:ETHUSDT"}')
    ws.send('{"type":"subscribe","symbol":"BINANCE:TRBUSDT"}')
    ws.send('{"type":"subscribe","symbol":"BINANCE:ONEUSDT"}')
    ws.send('{"type":"subscribe","symbol":"BINANCE:XRPUSDT"}')
    ws.send('{"type":"subscribe","symbol":"BINANCE:QNTUSDT"}')
    ws.send('{"type":"subscribe","symbol":"BINANCE:XLMUSDT"}')
    ws.send('{"type":"subscribe","symbol":"BINANCE:BCHUSDT"}')
    ws.send('{"type":"subscribe","symbol":"BINANCE:EOSUSDT"}')
    ws.send('{"type":"subscribe","symbol":"BINANCE:LTCUSDT"}')
    ws.send('{"type":"subscribe","symbol":"BINANCE:TRXUSDT"}')
    ws.send('{"type":"subscribe","symbol":"BINANCE:ETCUSDT"}')
    ws.send('{"type":"subscribe","symbol":"BINANCE:ADAUSDT"}')
    ws.send('{"type":"subscribe","symbol":"BINANCE:BNBUSDT"}')


if __name__ == "__main__":
    ws = websocket.WebSocketApp(f"wss://ws.finnhub.io?token={api_key}",
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error)
    ws.run_forever()
