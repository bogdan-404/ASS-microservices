from fastapi import FastAPI
import os, json, time, threading, psycopg2, pika, re

app = FastAPI()

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
        c.execute("SELECT id, pattern FROM bad_terms WHERE enabled=true")
        return [{"id": row[0], "pattern": row[1]} for row in c.fetchall()]

def word_count(s: str) -> int:
    return len([t for t in re.split(r"\W+", s) if t])

def has_words(s: str) -> bool:
    low = s.lower()
    for term in bad_terms:
        if term["pattern"].lower() in low:
            return True
    return False

def filter_and_collect_ids(content: str, terms: list) -> tuple:
    out = content
    low = content.lower()
    removed_ids = []
    for term in terms:
        pat = term["pattern"]
        pat_low = pat.lower()
        if pat_low in low:
            removed_ids.append(term["id"])
            regex = r"(?i)(?<!\w)" + re.escape(pat) + r"(?!\w)"
            out = re.sub(regex, " ", out)
            low = out.lower()
    out = re.sub(r"\s+", " ", out).strip()
    return out, removed_ids

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
            run_id = msg.get("runId")
            text_id = msg["textId"]
            content = msg["content"]
            wc = word_count(content)
            hw = has_words(content)
            score = wc * (2 if hw else 1)
            
            filtered_content, removed_ids = filter_and_collect_ids(content, bad_terms)
            removed_ids_str = "{" + ",".join(str(i) for i in removed_ids) + "}"

            with conn.cursor() as c:
                if run_id is not None:
                    c.execute("INSERT INTO results(text_id, run_id, word_count, has_words, score) VALUES(%s,%s,%s,%s,%s)",
                              (text_id, run_id, wc, hw, score))
                    c.execute("INSERT INTO filtered_strings(text_id, run_id, filtered_content, removed_bad_term_ids) VALUES(%s,%s,%s,%s)",
                              (text_id, run_id, filtered_content, removed_ids_str))
                else:
                    c.execute("INSERT INTO results(text_id, word_count, has_words, score) VALUES(%s,%s,%s,%s)",
                              (text_id, wc, hw, score))
                c.execute("UPDATE texts SET status='DONE' WHERE id=%s", (text_id,))
            conn.commit()

            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            conn.rollback()
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    channel.basic_consume(queue=RABBIT["queue"], on_message_callback=handle)
    channel.start_consuming()

threading.Thread(target=consumer, daemon=True).start()

