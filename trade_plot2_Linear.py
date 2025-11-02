import time
from pybit.unified_trading import WebSocket
import matplotlib.pyplot as plt
import matplotlib.animation as animation

# --- Configuration ---
SYMBOL = "POPCATUSDT"
INTERVAL_SECONDS = 3000

# --- Global variables ---
total_buy_volume = 0.0
total_sell_volume = 0.0
start_time = 0

# --- WebSocket Message Handler ---
def handle_trade_message(message):
    global total_buy_volume, total_sell_volume
    trade_events = message.get("data", [])
    for event in trade_events:
        try:
            volume = float(event.get('v'))
            side = event.get('S')
            if side == "Buy":
                total_buy_volume += volume
            elif side == "Sell":
                total_sell_volume += volume
        except (ValueError, TypeError):
            continue

# --- Animation Update Function ---
def update_plot(frame, ax):
    global start_time
    elapsed_time = time.time() - start_time
    if elapsed_time > INTERVAL_SECONDS:
        print("\nInterval complete. Closing plot to restart...")
        # Closing the plot causes a harmless traceback, which our main loop handles.
        plt.close() 
        return

    labels = ['Aggressive Buyers', 'Aggressive Sellers']
    volumes = [total_buy_volume, total_sell_volume]
    colors = ['green', 'red']

    ax.clear()
    bars = ax.bar(labels, volumes, color=colors)
    
    ax.set_ylabel('Total Volume (POPCAT) Perpetual')
    remaining_time = INTERVAL_SECONDS - elapsed_time
    ax.set_title(f'Real-Time Buy vs. Sell for {SYMBOL}\nTime Remaining: {remaining_time:.0f}s')
    ax.set_ylim(0, max(volumes) * 1.2 + 1)
    ax.grid(axis='y', linestyle='--', alpha=0.7)
    
    for bar in bars:
        yval = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2.0, yval, f'{yval:,.2f}', va='bottom', ha='center')

# --- Main Script Logic ---
if __name__ == "__main__":
    while True:
        total_buy_volume = 0.0
        total_sell_volume = 0.0
        start_time = time.time()
        
        print("Connecting to Bybit WebSocket...")
        ws = WebSocket(testnet=False, channel_type="linear")
        ws.trade_stream(symbol=SYMBOL, callback=handle_trade_message)
        print(f"✅ Connection successful. Starting new {int(INTERVAL_SECONDS/60)}-minute plot...")
        
        fig, ax = plt.subplots(figsize=(10, 6))
        
        # We add save_count=0 to remove the UserWarning
        ani = animation.FuncAnimation(fig, update_plot, fargs=(ax,), interval=500, save_count=0)
        
        plt.show()
        
        ws.exit()
        print("WebSocket disconnected. Preparing for the next interval...")
        time.sleep(3)