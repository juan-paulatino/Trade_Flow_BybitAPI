import time
from pybit.unified_trading import WebSocket
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from collections import OrderedDict

# --- Configuration ---
SYMBOL = "POPCATUSDT"
DEPTH = 200

# --- Global variables ---
bids = OrderedDict()
asks = OrderedDict()
snapshot_received = False

# --- WebSocket Message Handler (No changes here) ---
def handle_orderbook_message(message):
    global bids, asks, snapshot_received
    if message["type"] == "snapshot":
        bids.clear(); asks.clear()
        for p, q in message["data"]["b"]: bids[float(p)] = float(q)
        for p, q in message["data"]["a"]: asks[float(p)] = float(q)
        snapshot_received = True
        print("✅ Order book snapshot received. Plotting real-time depth...")
    elif message["type"] == "delta":
        if not snapshot_received: return
        for p, q in message["data"]["b"]:
            price, qty = float(p), float(q)
            if qty == 0:
                if price in bids: del bids[price]
            else: bids[price] = qty
        for p, q in message["data"]["a"]:
            price, qty = float(p), float(q)
            if qty == 0:
                if price in asks: del asks[price]
            else: asks[price] = qty

# --- Animation Update Function (UPDATED) ---
def update_plot(frame, ax_depth, ax_indicator):
    """This function is called repeatedly to redraw both the depth chart and the imbalance bar."""
    if not snapshot_received:
        ax_depth.set_title(f'Waiting for {SYMBOL} Order Book Snapshot...')
        return

    # --- 1. Update the Main Depth Chart (ax_depth) ---
    sorted_bids = OrderedDict(sorted(bids.items(), reverse=True))
    sorted_asks = OrderedDict(sorted(asks.items()))
    bid_prices = list(sorted_bids.keys())
    bid_vols = [sum(list(sorted_bids.values())[:i+1]) for i in range(len(sorted_bids))]
    ask_prices = list(sorted_asks.keys())
    ask_vols = [sum(list(sorted_asks.values())[:i+1]) for i in range(len(sorted_asks))]
    
    ax_depth.clear()
    ax_depth.step(bid_prices, bid_vols, color='green', where='pre', label='Bids (Buy Orders)')
    ax_depth.step(ask_prices, ask_vols, color='red', where='pre', label='Asks (Sell Orders)')
    ax_depth.fill_between(bid_prices, bid_vols, step="pre", alpha=0.2, color='green')
    ax_depth.fill_between(ask_prices, ask_vols, step="pre", alpha=0.2, color='red')
    ax_depth.set_title(f'Real-Time Order Book Depth for {SYMBOL} Perpetual')
    ax_depth.set_xlabel('Price (USDT)')
    ax_depth.set_ylabel('Cumulative Volume')
    ax_depth.legend(loc='upper left')
    ax_depth.grid(True, linestyle='--', alpha=0.6)
    
    if bid_prices and ask_prices:
        spread = ask_prices[0] - bid_prices[0]
        center = bid_prices[0] + spread / 2
        margin = max(spread * 5, 0.0005)
        ax_depth.set_xlim(center - margin, center + margin)

    # --- 2. Calculate and Update the Imbalance Indicator (ax_indicator) ---
    total_bid_volume = sum(bids.values())
    total_ask_volume = sum(asks.values())
    total_liquidity = total_bid_volume + total_ask_volume

    if total_liquidity > 0:
        bid_proportion = total_bid_volume / total_liquidity
        ask_proportion = total_ask_volume / total_liquidity
    else:
        bid_proportion, ask_proportion = 0.5, 0.5 # Default to 50/50 if no data

    ax_indicator.clear()
    # Draw the green "Bids" part of the bar
    ax_indicator.barh([0], [bid_proportion], color='green', height=1)
    # Draw the red "Asks" part, starting where the green part ends
    ax_indicator.barh([0], [ask_proportion], left=bid_proportion, color='red', height=1)
    
    # Add text labels for clarity
    ax_indicator.text(bid_proportion / 2, 0, f'Bids\n{bid_proportion:.1%}', color='white', ha='center', va='center', weight='bold')
    ax_indicator.text(bid_proportion + ask_proportion / 2, 0, f'Asks\n{ask_proportion:.1%}', color='white', ha='center', va='center', weight='bold')
    
    ax_indicator.set_title('Liquidity Imbalance')
    ax_indicator.set_xlim(0, 1)
    ax_indicator.set_yticks([]) # Hide the y-axis ticks

# --- Main Script Logic ---
if __name__ == "__main__":
    print(f"Connecting to Bybit WebSocket for {SYMBOL} order book...")
    ws = WebSocket(testnet=False, channel_type="linear")
    ws.orderbook_stream(depth=DEPTH, symbol=SYMBOL, callback=handle_orderbook_message)
    
    # Create the figure and two axes objects
    fig = plt.figure(figsize=(12, 8))
    # Main depth chart takes up most of the space
    ax_depth = plt.subplot2grid((6, 1), (0, 0), rowspan=5, colspan=1)
    # Indicator bar takes up the bottom space
    ax_indicator = plt.subplot2grid((6, 1), (5, 0), rowspan=1, colspan=1)
    
    plt.tight_layout(pad=3.0)

    ani = animation.FuncAnimation(fig, update_plot, fargs=(ax_depth, ax_indicator), interval=500, save_count=0)
    
    try:
        plt.show()
    except KeyboardInterrupt:
        print("\n🛑 Exiting script.")
    finally:
        ws.exit()
        print("WebSocket disconnected.")