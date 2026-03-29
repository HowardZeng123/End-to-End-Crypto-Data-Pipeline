import json
import websocket
from datetime import datetime
from kafka import KafkaProducer

# --- CẤU HÌNH KAFKA ---
# Chú ý: Tạm thời mình vẫn giữ nguyên tên topic là 'bitcoin_price' 
# để không làm hỏng thằng bốc vác Kafka Connect và MinIO của bro nhé.
KAFKA_TOPIC = 'bitcoin_price' 
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092', 'localhost:9093', 'localhost:9094']

print("🚀 Đang khởi động Kafka Producer Đa Luồng...")
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def on_message(ws, message):
    raw_data = json.loads(message)
    
    # Khi xài Combined Stream, Binance trả về dạng: {"stream": "btcusdt@ticker", "data": {...}}
    stream_name = raw_data['stream']
    data = raw_data['data']
    
    # Cắt chữ 'usdt@ticker' đi để lấy tên coin chuẩn (VD: 'btcusdt@ticker' -> 'BTC')
    symbol = stream_name.split('usdt')[0].upper()
    
    current_price = float(data['c'])
    event_time_ms = data['E']
    
    timestamp_sec = int(event_time_ms / 1000)
    ingest_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Đóng gói data chuẩn chỉnh
    payload = {
        'symbol': symbol,
        'price_usd': current_price,
        'timestamp': timestamp_sec,
        'ingest_time': ingest_time
    }

    # Bơm vào Kafka
    producer.send(KAFKA_TOPIC, payload)
    
    # Chọn icon cho đẹp terminal
    icons = {'BTC': '🟠', 'ETH': '🔷', 'SOL': '🟣', 'BNB': '🟡'}
    icon = icons.get(symbol, '🪙')
    
    print(f"{icon} [{symbol}] Đã bơm: ${current_price:,.2f}")

def on_error(ws, error):
    print(f"❌ Lỗi kết nối: {error}")

def on_close(ws, close_status_code, close_msg):
    print("🔴 Đã đóng kết nối với Binance")

def on_open(ws):
    print("🟢 Đã cắm SIÊU ỐNG HÚT đa luồng vào Binance! Bắt đầu xả lũ data...")

if __name__ == "__main__":
    # Gộp 4 luồng: BTC, ETH, SOL, BNB bằng dấu gạch chéo (/)
    COINS = "btcusdt@ticker/ethusdt@ticker/solusdt@ticker/bnbusdt@ticker"
    BINANCE_WS_URL = f"wss://stream.binance.com:9443/stream?streams={COINS}"
    
    ws = websocket.WebSocketApp(
        BINANCE_WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    
    ws.run_forever()