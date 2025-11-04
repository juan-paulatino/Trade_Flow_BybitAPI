import time
import threading
import datetime  # <-- CHANGED
from pybit.unified_trading import WebSocket
from collections import deque
import numpy as np

# --- Configuration ---
SYMBOL = "POPCATUSDT"
DEPTH = 1
# AGGREGATION_PERIOD_SECONDS is no longer needed, loop is clock-driven

# --- Global variables (Unchanged) ---
data_lock = threading.Lock()
bbo_updates = deque()
last_bid_price = 0.0
last_ask_price = 0.0

# --- WebSocket Message Handler (Unchanged) ---
def handle_bbo_message(message):
    global last_bid_price, last_ask_price
    
    if message["type"] == "snapshot":
        try:
            bid_data = message["data"]["b"]
            ask_data = message["data"]["a"]
            
            if not bid_data or not ask_data:
                return

            bid_price = float(bid_data[0][0])
            bid_size = float(bid_data[0][1])
            ask_price = float(ask_data[0][0])
            ask_size = float(ask_data[0][1])

            spread = ask_price - bid_price
            mid_price = (ask_price + bid_price) / 2.0
            total_bbo_volume = bid_size + ask_size
            
            if total_bbo_volume > 0:
                imbalance_ratio = bid_size / total_bbo_volume
            else:
                imbalance_ratio = 0.5 

            bid_tick = (bid_price != last_bid_price)
            ask_tick = (ask_price != last_ask_price)
            
            last_bid_price = bid_price
            last_ask_price = ask_price

            with data_lock:
                bbo_updates.append({
                    "time": time.time(), # This is a float (UNIX timestamp)
                    "mid_price": mid_price,
                    "spread": spread,
                    "imbalance_ratio": imbalance_ratio,
                    "bid_tick": bid_tick,
                    "ask_tick": ask_tick
                })

        except Exception as e:
            print(f"Error processing message: {e} - Data: {message}")

# --- Aggregation and Reporting Function (CHANGED) ---
def process_and_report():
    """
    This function is called by the main thread AT THE START of every minute
    (e.g., at 16:06:00.001) to process data from the PREVIOUS minute
    (e.g., 16:05:00.000 to 16:05:59.999).
    """
    
    # 1. Calculate the timestamps for the *previous* full minute
    now = datetime.datetime.now()
    # This is the timestamp for the *start* of the current minute (e.g., 16:06:00)
    current_minute_start_dt = now.replace(second=0, microsecond=0)
    # This is the timestamp for the *start* of the minute we want to report on (e.g., 16:05:00)
    period_start_dt = current_minute_start_dt - datetime.timedelta(minutes=1)
    
    # Get the float timestamps for comparison
    period_start_ts = period_start_dt.timestamp()
    period_end_ts = current_minute_start_dt.timestamp() # End is the start of the *next* minute
    
    # Get human-readable strings for the report
    period_start_str = period_start_dt.strftime('%H:%M:%S')
    # End time is 59 seconds (e.g., 16:05:59)
    period_end_str = (current_minute_start_dt - datetime.timedelta(seconds=1)).strftime('%H:%M:%S')

    # 2. Atomically drain the deque of all data from the target period
    updates_to_process = []
    with data_lock:
        # Efficiently pop all items that are OLDER than the start of the *current* minute
        # (i.e., everything from 16:05:59.999 and earlier)
        while bbo_updates and bbo_updates[0]["time"] < period_end_ts:
            update = bbo_updates.popleft()
            
            # Only append items that are *also* from *within* our target minute
            # (i.e., 16:05:00.000 or later)
            if update["time"] >= period_start_ts:
                updates_to_process.append(update)

    # 3. Process the snapshot
    if not updates_to_process:
        print(f"\n[{now.strftime('%H:%M:%S')}] No BBO updates received for period {period_start_str} - {period_end_str}.")
        return

    num_updates = len(updates_to_process)
    
    mid_prices = np.array([d['mid_price'] for d in updates_to_process])
    spreads = np.array([d['spread'] for d in updates_to_process])
    imbalances = np.array([d['imbalance_ratio'] for d in updates_to_process])
    
    bid_ticks = sum(d['bid_tick'] for d in updates_to_process)
    ask_ticks = sum(d['ask_tick'] for d in updates_to_process)
    total_ticks = bid_ticks + ask_ticks

    avg_mid_price = np.mean(mid_prices)
    mid_price_range = np.max(mid_prices) - np.min(mid_prices)
    avg_spread = np.mean(spreads)
    max_spread = np.max(spreads)
    avg_imbalance_pct = np.mean(imbalances) * 100

    # 4. Print the report
    print("\n" + "="*40)
    print(f"📈 1-Minute BBO Summary for {SYMBOL}")
    print(f"Period: {period_start_str} - {period_end_str}") # <-- CHANGED
    print(f"Total BBO Updates: {num_updates}")
    print(f"BBO Ticks (Price Changes): {total_ticks} (Bids: {bid_ticks}, Asks: {ask_ticks})")
    print("="*40)
    print(f"Price & Spread (USDT):")
    print(f"  - Avg. Mid-Price:    {avg_mid_price:.6f}")
    print(f"  - Mid-Price Range:   {mid_price_range:.6f}")
    print(f"  - Avg. Spread:       {avg_spread:.6f}")
    print(f"  - Max Spread Spike:  {max_spread:.6f}")
    print(f"Liquidity Ratio:")
    print(f"  - Avg. BBO Imbalance: {avg_imbalance_pct:.2f}%")
    print(f"    (>50% = Bids, <50% = Asks)")
    print("="*40 + "\n")


# --- Main Script Logic (CHANGED) ---
if __name__ == "__main__":
    print(f"Connecting to Bybit WebSocket for {SYMBOL} (Depth: {DEPTH})...")
    print(f"Aggregating data into 1-minute (00-59s) clock-aligned periods.") # <-- CHANGED
    
    ws = WebSocket(testnet=False, channel_type="linear")
    ws.orderbook_stream(depth=DEPTH, symbol=SYMBOL, callback=handle_bbo_message)
    
    # Wait a few seconds for the connection to establish and data to start flowing
    print("Waiting for first data...")
    time.sleep(5) 
    
    print("Starting clock-aligned aggregation loop...")
    try:
        while True:
            # 1. Calculate time until the *next* minute starts
            now = datetime.datetime.now()
            seconds_until_next_minute = 60 - (now.second + now.microsecond / 1_000_000)
            
            # 2. Sleep until that precise moment
            time.sleep(seconds_until_next_minute)
            
            # 3. At the start of the new minute (e.g., 16:06:00.000),
            #    process the data from the *previous* minute (16:05:00-16:05:59)
            process_and_report()
            
    except KeyboardInterrupt:
        print("\n🛑 Exiting script.")
    finally:
        ws.exit()
        print("WebSocket disconnected.")