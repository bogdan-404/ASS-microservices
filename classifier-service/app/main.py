from fastapi import FastAPI
from fastapi.responses import PlainTextResponse
import os, json, time, threading, psycopg2, pika
from prometheus_client import Counter, Histogram, generate_latest

app = FastAPI()
PROC_COUNT = Counter("classifier_processed_total", "Processed messages")
PROC_LAT = Histogram("classifier_item_duration_seconds", "Per-item processing time")

DB = dict(
  host=os.getenv("POSTGRES_HOST","db"),
  port=int(os.getenv("POSTGRES_PORT","5432")),
  db=os.getenv("POSTGRES_DB","tp_micro"),
  user=os.getenv("POSTGRES_USER","tp_user"),
  pwd=os.getenv("POSTGRES_PASSWORD","tp_pass"),
)
RABBIT = dict(
  host=os.getenv("RABBIT_HOST","rabbit"),
  port=int(os.getenv("RABBIT_PORT","5672")),
  user=os.getenv("RABBIT_USER","guest"),
  pwd=os.getenv("RABBIT_PASS","guest"),
  queue=os.getenv("RABBIT_QUEUE","incoming_texts"),
  prefetch=int(os.getenv("RABBIT_PREFETCH","50")),
)

bad_terms = []

def load_bad_terms(conn):
    with conn.cursor() as c:
        c.execute("SELECT pattern FROM bad_terms WHERE enabled=true")
        return [row[0].lower() for row in c.fetchall()]

def word_count(s: str) -> int:
    import re
    return len([t for t in re.split(r"\W+", s) if t])

def has_words(s: str) -> bool:
    low = s.lower()
    for p in bad_terms:
        if p in low:
            return True
    return False

def consumer():
    conn = psycopg2.connect(host=DB["host"], port=DB["port"], dbname=DB["db"],
                            user=DB["user"], password=DB["pwd"])
    global bad_terms
    bad_terms = load_bad_terms(conn)

    credentials = pika.PlainCredentials(RABBIT["user"], RABBIT["pwd"])
    params = pika.ConnectionParameters(RABBIT["host"], RABBIT["port"], "/", credentials)
    channel = pika.BlockingConnection(params).channel()
    channel.queue_declare(queue=RABBIT["queue"], durable=True)
    channel.basic_qos(prefetch_count=RABBIT["prefetch"])

    def handle(ch, method, props, body):
        start = time.time()
        try:
            msg = json.loads(body)
            text_id = msg["textId"]
            content = msg["content"]
            wc = word_count(content)
            hw = has_words(content)
            score = wc * (2 if hw else 1)

            with conn.cursor() as c:
                c.execute("INSERT INTO results(text_id, word_count, has_words, score) VALUES(%s,%s,%s,%s)",
                          (text_id, wc, hw, score))
                c.execute("UPDATE texts SET status='DONE' WHERE id=%s", (text_id,))
            conn.commit()

            PROC_COUNT.inc()
            PROC_LAT.observe(time.time() - start)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception:
            conn.rollback()
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    channel.basic_consume(queue=RABBIT["queue"], on_message_callback=handle)
    channel.start_consuming()

threading.Thread(target=consumer, daemon=True).start()

@app.get("/metrics")
def metrics():
    return PlainTextResponse(generate_latest().decode("utf-8"))

