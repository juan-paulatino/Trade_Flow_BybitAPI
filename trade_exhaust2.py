import logging
import time
import collections
import statistics
import datetime
from pybit.unified_trading import WebSocket

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class OrderFlowTracker:
    def __init__(self, symbol, cvd_zscore_period=100):
        self.symbol = symbol
        self.ws = None
        
        self.trade_buffer = [] 
        
        # Inter-candle state
        self.cumulative_volume_delta = 0.0
        self.cvd_history = collections.deque(maxlen=cvd_zscore_period)
        self.cvd_zscore_period = cvd_zscore_period
        
        self.last_kline_start_time = None

    def connect(self):
        logging.info("Connecting to Bybit WebSocket...")
        self.ws = WebSocket(
            testnet=False,
            channel_type="linear",
        )
        self.ws.kline_stream(interval=1, symbol=self.symbol, callback=self.handle_kline_message)
        self.ws.trade_stream(symbol=self.symbol, callback=self.handle_trade_message)
        logging.info(f"Subscribed to kline.1.{self.symbol} and publicTrade.{self.symbol}")

    def run(self):
        self.connect()
        while True:
            time.sleep(60)

    def handle_trade_message(self, message):
        """Callback function to handle incoming trade messages."""
        for trade in message.get("data", []):
            self.trade_buffer.append(trade)

    def handle_kline_message(self, message):
        """Callback function to handle incoming kline messages."""
        kline_data_list = message.get("data", [])
        if not kline_data_list:
            return

        candle = kline_data_list[0] 
        
        if candle.get("confirm") is True:
            if self.last_kline_start_time != candle['start']:
                self.last_kline_start_time = candle['start']
                self.process_candle_data(candle)

    def process_candle_data(self, candle):
        """Processes the buffered trades for a completed candle."""
        candle_start_ms = int(candle['start'])
        candle_end_ms = int(candle['end'])
        candle_close_price = float(candle['close'])

        # Convert timestamp
        dt_object = datetime.datetime.utcfromtimestamp(candle_start_ms / 1000.0)
        formatted_timestamp = dt_object.strftime('%Y-%m-%d %H:%M:%S,') + f"{dt_object.microsecond // 1000:03d}"

        # Step 1: Aggregate Intra-Candle Volumes
        buy_volume = 0.0
        sell_volume = 0.0
        
        relevant_trades = [
            t for t in self.trade_buffer 
            if candle_start_ms <= int(t['T']) <= candle_end_ms
        ]

        for trade in relevant_trades:
            volume = float(trade['v'])
            if trade['S'] == 'Buy':
                buy_volume += volume
            elif trade['S'] == 'Sell':
                sell_volume += volume
        
        # Step 2: Calculate Per-Minute Volume Delta
        volume_delta = buy_volume - sell_volume

        # Step 3: Calculate Aggression Ratio
        total_volume = buy_volume + sell_volume
        # --- MODIFIED FORMULA ---
        # If total_volume is 0, we set to 0.5 (a neutral tie)
        agg_ratio = (buy_volume / total_volume) if total_volume > 0 else 0.5

        # Step 4: Update Cumulative Volume Delta (CVD)
        self.cumulative_volume_delta += volume_delta
        self.cvd_history.append(self.cumulative_volume_delta)

        # Step 5: Calculate CVD Z-Score Ratio
        cvd_zscore_ratio = 0.0
        if len(self.cvd_history) > 1:
            try:
                mean_cvd = statistics.mean(self.cvd_history)
                stdev_cvd = statistics.stdev(self.cvd_history)
                if stdev_cvd > 0:
                    cvd_zscore_ratio = (self.cumulative_volume_delta - mean_cvd) / stdev_cvd
            except statistics.StatisticsError:
                cvd_zscore_ratio = 0.0


        # --- MODIFIED: Update the output dictionary format ---
        output = {
            "timestamp_utc": formatted_timestamp,
            "close_price": candle_close_price,
            "buy_volume": round(buy_volume, 4),
            "sell_volume": round(sell_volume, 4),
            "total_volume": round(total_volume, 4),
            "delta": round(volume_delta, 4),
            "agg_ratio": round(agg_ratio, 4),        # <-- CHANGED
            "cumulative_volume_delta": round(self.cumulative_volume_delta, 4),
            "cvd_zscore_ratio": round(cvd_zscore_ratio, 4)
        }
        logging.info(output)

        # Clean up the buffer
        self.trade_buffer = [
            t for t in self.trade_buffer 
            if int(t['T']) > candle_end_ms
        ]

if __name__ == "__main__":
    tracker = OrderFlowTracker(symbol="POPCATUSDT") 
    tracker.run()

