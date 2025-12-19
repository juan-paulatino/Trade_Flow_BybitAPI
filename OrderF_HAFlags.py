import logging
import time
import collections
import statistics
import datetime
import threading
from pybit.unified_trading import WebSocket

# --- ALERT AND STRATEGY THRESHOLDS (CONFIGURABLE) ---
GREEN_ZONE_THRESHOLD = 0.55
RED_ZONE_THRESHOLD = 0.35
CONSOLIDATION_WATCH_PERIOD = 3  # Number of candles to watch after an initial failure
ALERT_COOLDOWN_PERIOD = 5       # Number of candles to wait before firing a new alert

# Configure logging
# Use WARNING level for alerts to make them stand out
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class OrderFlowTracker:
    def __init__(self, symbol):
        self.symbol = symbol
        self.main_ws = None
        self.depth_ws = None
        
        # --- Data Buffers ---
        self.trade_data_lock = threading.Lock()
        self.trade_buffer = collections.deque()
        self.bbo_data_lock = threading.Lock()
        self.bbo_buffer = collections.deque() 
        self.book_lock = threading.Lock()
        self.live_bids = {}
        self.live_asks = {}
        self.is_book_ready = False
        
        self.last_kline_start_time = None
        
        # --- Heiken Ashi State ---
        self.last_ha_open = None
        self.last_ha_close = None

        # --- NEW: Trade Setup State Management ---
        self.ha_green_streak = 0
        self.consolidation_watch_countdown = 0
        self.alert_cooldown = 0
        self.last_candle_data = None


    def _check_entry_conditions(self, current_data):
        """Helper method to check if the current candle meets all high-probability criteria."""
        
        # 1. HA_color must be Green
        if current_data.get('HA_color') != 'Green':
            return False

        agg_ratio = current_data.get('agg_ratio')
        avg_bbo_imba = current_data.get('avg_bbo_imba')

        # 2. No metrics in the Red Zone
        if agg_ratio < RED_ZONE_THRESHOLD or avg_bbo_imba < RED_ZONE_THRESHOLD:
            return False
            
        # 3. At least one metric must be in the Green Zone (Conviction Check)
        if agg_ratio < GREEN_ZONE_THRESHOLD and avg_bbo_imba < GREEN_ZONE_THRESHOLD:
            return False

        # 4. Price agreement (must not make a significant lower low)
        if self.last_candle_data and current_data.get('close_price') < self.last_candle_data.get('close_price'):
             # This condition can be adjusted for more tolerance if needed
             pass # For now, we allow minor pullbacks if order flow is strong

        return True


    def _run_trade_setup_logic(self, current_data):
        """Main logic to identify and alert on trade setups."""
        
        # Decrement cooldown timer
        if self.alert_cooldown > 0:
            self.alert_cooldown -= 1

        # --- Update Heiken Ashi Streak ---
        if current_data.get('HA_color') == 'Green':
            self.ha_green_streak += 1
        else:
            # If the trend breaks, reset everything
            self.ha_green_streak = 0
            self.consolidation_watch_countdown = 0

        # Don't check for trades if a recent alert was fired
        if self.alert_cooldown > 0:
            return

        # --- "Strict 3+1" Protocol Check ---
        if self.ha_green_streak == 4:
            if self._check_entry_conditions(current_data):
                logging.warning(f"*** TRADE SETUP: Strict '3+1' Entry Triggered at {current_data['utc']} ***")
                self.alert_cooldown = ALERT_COOLDOWN_PERIOD
                self.consolidation_watch_countdown = 0 # Turn off consolidation watch
                return
            else:
                # If the strict check fails, start the consolidation watch
                logging.info(f"Strict '3+1' failed. Entering Consolidation Watch for {CONSOLIDATION_WATCH_PERIOD} candles.")
                self.consolidation_watch_countdown = CONSOLIDATION_WATCH_PERIOD
        
        # --- "Consolidation Watch" Protocol Check ---
        elif self.consolidation_watch_countdown > 0:
            if self._check_entry_conditions(current_data):
                logging.warning(f"*** TRADE SETUP: 'Consolidation Watch' Entry Triggered at {current_data['utc']} ***")
                self.alert_cooldown = ALERT_COOLDOWN_PERIOD
                self.consolidation_watch_countdown = 0 # Reset after firing
                return
            else:
                self.consolidation_watch_countdown -= 1
                if self.consolidation_watch_countdown == 0:
                    logging.info("Consolidation Watch period ended. No valid entry found.")


    def process_candle_data(self, candle):
        """Processes all buffered data for the completed candle and runs setup logic."""
        candle_start_ms = int(candle['start'])
        candle_end_ms = int(candle['end'])
        
        # --- Heiken Ashi Full Calculation ---
        open_price = float(candle['open'])
        high_price = float(candle['high'])
        low_price = float(candle['low'])
        close_price = float(candle['close'])

        ha_close = (open_price + high_price + low_price + close_price) / 4.0

        if self.last_ha_open is None: # First candle initialization
            ha_open = (open_price + close_price) / 2.0
        else:
            ha_open = (self.last_ha_open + self.last_ha_close) / 2.0
        
        ha_color = "Green" if ha_close > ha_open else "Red"

        # Update state for the next candle's calculation
        self.last_ha_open = ha_open
        self.last_ha_close = ha_close
        
        dt_object = datetime.datetime.utcfromtimestamp(candle_start_ms / 1000.0)
        formatted_timestamp = dt_object.strftime('%Y-%m-%d %H:%M:%S,') + f"{dt_object.microsecond // 1000:03d}"

        # --- Drain buffers and process data (condensed for brevity) ---
        with self.trade_data_lock:
            relevant_trades = [t for t in self.trade_buffer if candle_start_ms <= int(t['T']) <= candle_end_ms]
            self.trade_buffer = collections.deque(t for t in self.trade_buffer if int(t['T']) > candle_end_ms)
        with self.bbo_data_lock:
            relevant_bbo_updates = [b for b in self.bbo_buffer if candle_start_ms <= b['ts'] <= candle_end_ms]
            self.bbo_buffer = collections.deque(b for b in self.bbo_buffer if b['ts'] > candle_end_ms)

        buy_volume = sum(float(t['v']) for t in relevant_trades if t['S'] == 'Buy')
        sell_volume = sum(float(t['v']) for t in relevant_trades if t['S'] == 'Sell')
        total_volume = buy_volume + sell_volume
        agg_ratio = (buy_volume / total_volume) if total_volume > 0 else 0.5
        avg_bbo_imba = statistics.mean([b['imbalance'] for b in relevant_bbo_updates]) if relevant_bbo_updates else 0.5

        # --- Log Combined Output ---
        output_data = {
            "utc": formatted_timestamp,
            "close_price": close_price,
            "HA_open": round(ha_open, 5),
            "HA_close": round(ha_close, 5),
            "HA_color": ha_color,
            "agg_ratio": round(agg_ratio, 4),
            "avg_bbo_imba": round(avg_bbo_imba, 4),
            # Other metrics can be added here if needed for logging
        }
        logging.info(output_data)

        # --- NEW: Run the setup detection logic ---
        self._run_trade_setup_logic(output_data)

        # Store data for the next candle's comparison
        self.last_candle_data = output_data

    # --- The rest of the class methods (connect, run, handle_*, etc.) remain the same ---
    # ... (omitting for brevity, but they are identical to the previous script) ...
    def connect(self):
        depth_thread = threading.Thread(target=self._connect_depth_book, daemon=True)
        depth_thread.start()
        logging.info("Connecting Main WebSocket (Kline, Trade, BBO)...")
        while True:
            try:
                self.main_ws = WebSocket(testnet=False, channel_type="linear")
                self.main_ws.kline_stream(interval=1, symbol=self.symbol, callback=self.handle_kline_message)
                self.main_ws.trade_stream(symbol=self.symbol, callback=self.handle_trade_message)
                self.main_ws.orderbook_stream(depth=1, symbol=self.symbol, callback=self.handle_bbo_message)
                logging.info(f"Main WS Subscribed to kline.1, publicTrade, and orderbook.1 for {self.symbol}")
                break
            except Exception as e:
                logging.error(f"Failed to connect to Main WebSocket: {e}. Retrying in 30 seconds...")
                time.sleep(30)

    def _connect_depth_book(self):
        logging.info("Connecting Depth WebSocket (Orderbook.50)...")
        while True:
            try:
                self.depth_ws = WebSocket(testnet=False, channel_type="linear")
                self.depth_ws.orderbook_stream(depth=50, symbol=self.symbol, callback=self.handle_depth_book_message)
                logging.info(f"Depth WS Subscribed to orderbook.50 for {self.symbol}")
                while True: time.sleep(60)
            except Exception as e:
                logging.error(f"Failed to connect to Depth WebSocket: {e}. Retrying in 30 seconds...")
                time.sleep(30)

    def run(self):
        self.connect()
        while True: time.sleep(60)

    def handle_trade_message(self, message):
        with self.trade_data_lock:
            for trade in message.get("data", []): self.trade_buffer.append(trade)

    def handle_bbo_message(self, message):
        try:
            data = message.get("data", {})
            bids, asks = data.get("b", []), data.get("a", [])
            if not bids or not asks: return
            bid_size, ask_size = float(bids[0][1]), float(asks[0][1])
            total_size = bid_size + ask_size
            imbalance = bid_size / total_size if total_size > 0 else 0.5
            with self.bbo_data_lock: self.bbo_buffer.append({"ts": int(message.get("ts")), "imbalance": imbalance})
        except Exception as e: logging.error(f"Error processing BBO message: {e} - Data: {message}")

    def handle_depth_book_message(self, message):
        message_type, data = message.get("type"), message.get("data", {})
        try:
            with self.book_lock:
                if message_type == "snapshot":
                    self.live_bids = {b[0]: float(b[1]) for b in data.get("b", [])}
                    self.live_asks = {a[0]: float(a[1]) for a in data.get("a", [])}
                    self.is_book_ready = True
                elif message_type == "delta":
                    for b in data.get("b", []):
                        if float(b[1]) == 0: self.live_bids.pop(b[0], None)
                        else: self.live_bids[b[0]] = float(b[1])
                    for a in data.get("a", []):
                        if float(a[1]) == 0: self.live_asks.pop(a[0], None)
                        else: self.live_asks[a[0]] = float(a[1])
        except Exception as e: logging.error(f"Error processing Depth 50 message: {e} - Data: {message}")

    def handle_kline_message(self, message):
        candle = message.get("data", [])[0]
        if candle.get("confirm") and self.last_kline_start_time != candle['start']:
            self.last_kline_start_time = candle['start']
            self.process_candle_data(candle)


if __name__ == "__main__":
    tracker = OrderFlowTracker(symbol="POPCATUSDT")
    tracker.run()

