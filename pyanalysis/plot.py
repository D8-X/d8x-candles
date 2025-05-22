import pandas as pd
import json
import matplotlib.pyplot as plt
from mplfinance.original_flavor import candlestick_ohlc
import matplotlib.dates as mdates
from matplotlib.ticker import MaxNLocator


def plot_candles(timestamps, opens, highs, lows, closes):
    fig, ax = plt.subplots(figsize=(10, 6))
    # Width of the candlesticks
    width = 0.02

    for i, time in enumerate(timestamps):
        color = 'g' if closes[i] > opens[i] else 'r'  # Green for bullish, red for bearish
        # Draw the body (rectangle)
        ax.add_patch(plt.Rectangle((time - width / 2, min(opens[i], closes[i])),
                                    width, abs(opens[i] - closes[i]),
                                    edgecolor=color, facecolor=color))
        # Draw the wicks (lines)
        ax.plot([time, time], [lows[i], highs[i]], color=color)

    # Format the x-axis
    ax.xaxis_date()
    #ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))
    ax.set_xlabel('Time')
    ax.set_ylabel('Price')
    plt.title('Candlestick Chart')
    plt.grid()

    plt.savefig("tmp1.png")

if __name__=="__main__":
    with open("./pyanalysis/data.json") as f:
        data = json.load(f)
    
    #ohlc = []
    #for item in d:
    #    ohlc.append([item['start']/1000, item['open'], item['high'], item['low'], item['close']])
    timestamps = [item["start"]/1000 for item in data]
    opens = [item["open"] for item in data]
    highs = [item["high"] for item in data]
    lows = [item["low"] for item in data]
    closes = [item["close"] for item in data]

    plt.plot(timestamps)
    plt.title("timestamps")
    plt.savefig("tmp0.png")

    boxplot_data = [highs, opens, lows, closes]
    # Plot the boxplot
    fig, ax = plt.subplots(figsize=(8, 6))

    # Create the boxplot
    ax.boxplot(boxplot_data, patch_artist=True,
            boxprops=dict(facecolor="lightblue", color="blue"),
            medianprops=dict(color="red"))

    # Set x-axis labels and title
    ax.set_xticks([1, 2, 3, 4])
    ax.set_xticklabels(['Highs', 'Opens', 'Lows', 'Closes'])
    


    plt.savefig("tmp.png")
    # Plot the candlestick chart
    #plot_candles(timestamps, opens, highs, lows, closes)
