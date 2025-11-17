import sys
import threading
import queue
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import mplfinance as mpf
from pybit.unified_trading import WebSocket
from time import sleep

# --- Global Variables ---
SYMBOL = "POPCATUSDT"
INTERVAL = 1
MAX_CANDLES_DISPLAY = 60  # Number of candles to display on the chart
data_queue = queue.Queue() # Thread-safe queue for websocket data

# This DataFrame will store all our raw candle data
# We need to keep a history for accurate Heikin Ashi calculation
historical_df = pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
historical_df = historical_df.set_index('timestamp')

# --- 1. WebSocket Data Handling ---

def handle_websocket_message(message):
    """
    Callback function to handle incoming kline data from the websocket.
    It puts the received candle data into a thread-safe queue.
    """
    # Extract the candle data from the message
    # The structure is {"topic": "...", "data": [ {...} ], ...}
    candle = message.get("data", [{}])[0]
    
    # Ensure the candle has data before processing
    if "start" in candle:
        data_queue.put(candle)

def start_websocket_thread():
    """
    Initializes and starts the Bybit WebSocket connection in a separate thread.
    """
    print(f"Attempting to connect to WebSocket for {SYMBOL}...")
    try:
        ws = WebSocket(
            testnet=False,
            channel_type="linear",
        )
        ws.kline_stream(
            interval=INTERVAL,
            symbol=SYMBOL,
            callback=handle_websocket_message
        )
        print(f"Successfully subscribed to {SYMBOL} kline stream.")
    except Exception as e:
        print(f"Error connecting to WebSocket: {e}")
        sys.exit(1)
        
    # Keep the thread alive (the pybit WebSocket runs in its own daemon threads)
    while True:
        sleep(1)

# --- 2. Heikin Ashi Calculation ---

def calculate_heikin_ashi(df_in):
    """
    Calculates Heikin Ashi candles from a standard OHLC DataFrame.
    
    This calculation *depends on the previous candle*, which is why we must
    calculate it on the full historical DataFrame, not just the segment
    we are about to plot.
    """
    if df_in.empty:
        return df_in

    ha_df = pd.DataFrame(index=df_in.index)

    # HA Close: (Open + High + Low + Close) / 4
    ha_df['close'] = (df_in['open'] + df_in['high'] + df_in['low'] + df_in['close']) / 4

    # HA Open: (Previous HA Open + Previous HA Close) / 2
    # For the first candle, HA Open = (Open + Close) / 2
    ha_df['open'] = 0.0
    if not ha_df.empty:
        ha_df.iloc[0, ha_df.columns.get_loc('open')] = (df_in.iloc[0]['open'] + df_in.iloc[0]['close']) / 2
    
    for i in range(1, len(ha_df)):
        ha_df.iloc[i, ha_df.columns.get_loc('open')] = \
            (ha_df.iloc[i-1]['open'] + ha_df.iloc[i-1]['close']) / 2

    # HA High: Max(High, HA Open, HA Close)
    ha_df['high'] = ha_df[['open', 'close']].join(df_in['high']).max(axis=1)

    # HA Low: Min(Low, HA Open, HA Close)
    ha_df['low'] = ha_df[['open', 'close']].join(df_in['low']).min(axis=1)
    
    # Volume is not averaged
    ha_df['volume'] = df_in['volume']

    return ha_df

# --- 3. Plotting & Animation ---

# Set up the plot
fig, (ax1, ax2) = plt.subplots(2, 1, sharex=True, figsize=(12, 8))
fig.suptitle(f'{SYMBOL} 1-Minute Live Chart', fontsize=16)

# Create a custom style for mplfinance
mpf_style = mpf.make_mpf_style(base_mpf_style='charles',
                               marketcolors=mpf.make_marketcolors(up='g', down='r', inherit=True),
                               gridstyle=':',
                               y_on_right=True)

# Set titles for the subplots
ax1.set_title("Standard Candles")
ax1.set_ylabel("Price (USDT)")
ax2.set_title("Heikin Ashi")
ax2.set_ylabel("Price (USDT)")
plt.xlabel("Time")


def process_queue_data():
    """
    Processes all new data from the queue and updates the global historical_df.
    This handles both new candles and updates to the current (unconfirmed) candle.
    """
    global historical_df
    
    while not data_queue.empty():
        try:
            candle = data_queue.get_nowait()
            
            # Convert data to correct types
            timestamp = pd.to_datetime(int(candle['start']), unit='ms')
            new_data = {
                'open': float(candle['open']),
                'high': float(candle['high']),
                'low': float(candle['low']),
                'close': float(candle['close']),
                'volume': float(candle['volume']),
            }

            # Update existing candle or append new one
            # 'confirm: false' means the candle is still active
            # 'confirm: true' means the candle is closed
            historical_df.loc[timestamp] = new_data
            
        except queue.Empty:
            break
        except Exception as e:
            print(f"Error processing queue data: {e}")
            
    # Sort by timestamp just in case and remove duplicates
    if not historical_df.empty:
        historical_df = historical_df.sort_index().drop_duplicates(keep='last')

def animate(i):
    """
    The main animation function, called repeatedly by FuncAnimation.
    """
    global historical_df
    
    # 1. Process all new data from the websocket
    process_queue_data()
    
    # Don't plot if we have no data
    if historical_df.empty or len(historical_df) < 2:
        print("Waiting for data...")
        return

    # 2. Get the *last* MAX_CANDLES_DISPLAY for standard plotting
    plot_df = historical_df.iloc[-MAX_CANDLES_DISPLAY:]

    # 3. Calculate Heikin Ashi on the *entire* history
    #    This is crucial for the dependency to be correct
    ha_full_df = calculate_heikin_ashi(historical_df)
    
    # 4. Get the *last* MAX_CANDLES_DISPLAY of the HA data for plotting
    plot_ha_df = ha_full_df.iloc[-MAX_CANDLES_DISPLAY:]

    # 5. Clear and redraw the plots
    ax1.clear()
    ax2.clear()
    
    # Re-apply formatting as clear() removes it
    ax1.set_title("Standard Candles")
    ax1.set_ylabel("Price (USDT)")
    ax2.set_title("Heikin Ashi")
    ax2.set_ylabel("Price (USDT)")

    # Plot using mplfinance
    mpf.plot(plot_df, ax=ax1, type='candle', style=mpf_style, volume=ax1.twinx())
    mpf.plot(plot_ha_df, ax=ax2, type='candle', style=mpf_style, volume=ax2.twinx())
    
    # Improve x-axis labels
    for label in ax2.get_xticklabels():
        label.set_rotation(30)
        label.set_ha('right')
        
    plt.tight_layout(rect=[0, 0.03, 1, 0.95]) # Adjust layout

# --- 4. Main Execution ---

if __name__ == "__main__":
    # Start the websocket connection in a background thread
    # 'daemon=True' ensures the thread will close when the main program exits
    ws_thread = threading.Thread(target=start_websocket_thread, daemon=True)
    ws_thread.start()
    
    # Give the websocket a second to connect
    print("Waiting for WebSocket connection (2s)...")
    sleep(2)

    # Start the animation
    print("Starting animation...")
    ani = animation.FuncAnimation(fig, animate, interval=1000) # Update every 1000ms (1 second)
    
    try:
        plt.show()
    except KeyboardInterrupt:
        print("Animation stopped.")
        sys.exit(0)
