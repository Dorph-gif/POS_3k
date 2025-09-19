from prometheus_client import start_http_server, Counter
import threading
import os

METRICS_PORT = int(os.getenv("METRICS_PORT", 8001))

notifications_counter = Counter(
    'scrapper_notifications_total',
    'Количество полученных уведомлений'
)

def start_metrics_server():
    thread = threading.Thread(target=start_http_server, args=(METRICS_PORT,))
    thread.daemon = True
    thread.start()