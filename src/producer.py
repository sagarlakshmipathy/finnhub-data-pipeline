import json
import websocket
import boto3
from confluent_kafka import Producer

sm_client = boto3.client("secretsmanager")
ssm_client = boto3.client("ssm")

api_key = sm_client.get_secret_value(SecretId="finnhub-api-key")["SecretString"]
kafka_topic = ssm_client.get_parameter(Name="finnhub-kafka-topic")["Parameter"]["Value"]
bootstrap_server = ssm_client.get_parameter(Name="finnhub-bootstrap-server")["Parameter"]["Value"]

kafka_config = {
    "bootstrap.servers": bootstrap_server,
    "client.id": "finnhub-producer"
}
producer = Producer(kafka_config)


def on_message(ws, message):
    json_message = json.loads(message)
    message_data = json_message["data"]
    for transaction in message_data:
        if transaction != "data":
            print(transaction)
            producer.produce(kafka_topic, json.dumps(transaction))


def on_error(ws, error):
    print(error)


def on_open(ws):
    print("opened")
    # ws.send('{"type":"subscribe","symbol":"AAPL"}')
    # ws.send('{"type":"subscribe","symbol":"AMZN"}')
    ws.send('{"type":"subscribe","symbol":"BINANCE:ETHUSDT"}')
    ws.send('{"type":"subscribe","symbol":"BINANCE:BTCUSDT"}')
    ws.send('{"type":"subscribe","symbol":"BINANCE:XRPUSDT"}')


if __name__ == "__main__":
    ws = websocket.WebSocketApp(f"wss://ws.finnhub.io?token={api_key}",
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error)
    ws.run_forever()
