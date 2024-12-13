import websocket
import json
from plot import plot_candles 
from datetime import datetime

def connect_and_send(range):
    uri = "ws://127.0.0.1:8081/ws"
    message = {
        "type": "subscribe",
        "topic": "BTC-USD:"+range
    }

    try:
        # Connect to the WebSocket
        ws = websocket.create_connection(uri)
        print("Connected to the WebSocket server.")

        # Send a message
        ws.send(json.dumps(message))
        print(f"Sent: {message}")

        # Receive a response
        response = ws.recv()
        print(f"Received")

        # Process the response
        data = json.loads(response)

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Disconnect
        ws.close()
        print("Disconnected from the WebSocket server.")
        return data['data']

def to_date(ts):
    return datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

if __name__=="__main__":
    range_choice =["1m","5m","15m","1h","1d"]
    val=1
    for k in range(1,100):
        ch = range_choice[int(val)-1]
        data = connect_and_send(ch)
        timestamps = [item["start"]/1000 for item in data]
        opens = [item["open"] for item in data]
        highs = [item["high"] for item in data]
        lows = [item["low"] for item in data]
        closes = [item["close"] for item in data]
        print(f"{k}: {to_date(int(timestamps[0]))}-{to_date(int(timestamps[len(timestamps)-1]))}, #obs = ${len(timestamps)}")
        plot_candles(timestamps, opens, highs, lows, closes)
        print('\nspecify next range 1:"1m", 2:"5m", 3:"15m", 4:"1h", 5:"1d"')
        val = input()
        