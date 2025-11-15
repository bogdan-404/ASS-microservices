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
    # Wait for RabbitMQ to be ready
    max_retries = 30
    retry_delay = 2
    connection = None
    
    for attempt in range(max_retries):
        try:
            credentials = pika.PlainCredentials(RABBIT["user"], RABBIT["pwd"])
            params = pika.ConnectionParameters(
                RABBIT["host"], 
                RABBIT["port"], 
                "/", 
                credentials,
                connection_attempts=1,
                retry_delay=1,
                socket_timeout=5
            )
            connection = pika.BlockingConnection(params)
            print(f"Successfully connected to RabbitMQ on attempt {attempt + 1}")
            break
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"Waiting for RabbitMQ... (attempt {attempt + 1}/{max_retries}): {e}")
                time.sleep(retry_delay)
            else:
                print(f"Failed to connect to RabbitMQ after {max_retries} attempts: {e}")
                return
    
    if connection is None:
        print("No connection to RabbitMQ, consumer exiting")
        return
    
    # Connect to database
    conn = psycopg2.connect(host=DB["host"], port=DB["port"], dbname=DB["db"],
                            user=DB["user"], password=DB["pwd"])
    global bad_terms
    bad_terms = load_bad_terms(conn)

    channel = connection.channel()
    channel.queue_declare(queue=RABBIT["queue"], durable=True)
    channel.basic_qos(prefetch_count=RABBIT["prefetch"])

    def handle(ch, method, props, body):
        nonlocal conn
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

            # Use connection directly - psycopg2 handles reconnection automatically
            try:
                with conn.cursor() as c:
                    if run_id is not None:
                        c.execute("INSERT INTO results(text_id, run_id, word_count, has_words, score) VALUES(%s,%s,%s,%s,%s)",
                                  (text_id, run_id, wc, hw, score))
                        c.execute("INSERT INTO filtered_strings(text_id, run_id, filtered_content, removed_bad_term_ids) VALUES(%s,%s,%s,%s)",
                                  (text_id, run_id, filtered_content, removed_ids_str))
                    else:
                        c.execute("INSERT INTO results(text_id, word_count, has_words, score) VALUES(%s,%s,%s,%s)",
                                  (text_id, wc, hw, score))
                conn.commit()
            except psycopg2.OperationalError:
                # Connection lost, reconnect
                try:
                    conn.close()
                except:
                    pass
                conn = psycopg2.connect(host=DB["host"], port=DB["port"], dbname=DB["db"],
                                        user=DB["user"], password=DB["pwd"])
                # Retry the insert
                with conn.cursor() as c:
                    if run_id is not None:
                        c.execute("INSERT INTO results(text_id, run_id, word_count, has_words, score) VALUES(%s,%s,%s,%s,%s)",
                                  (text_id, run_id, wc, hw, score))
                        c.execute("INSERT INTO filtered_strings(text_id, run_id, filtered_content, removed_bad_term_ids) VALUES(%s,%s,%s,%s)",
                                  (text_id, run_id, filtered_content, removed_ids_str))
                    else:
                        c.execute("INSERT INTO results(text_id, word_count, has_words, score) VALUES(%s,%s,%s,%s)",
                                  (text_id, wc, hw, score))
                conn.commit()

            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            import traceback
            print(f"ERROR processing message: {e}")
            print(traceback.format_exc())
            try:
                conn.rollback()
            except:
                pass
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    try:
        channel.basic_consume(queue=RABBIT["queue"], on_message_callback=handle)
        print(f"Classifier consumer started, listening on queue: {RABBIT['queue']}")
        channel.start_consuming()
    except Exception as e:
        print(f"Error in consumer: {e}")
    finally:
        if connection and not connection.is_closed:
            connection.close()
        if conn:
            conn.close()

# Start consumer in background thread (one per container instance)
threading.Thread(target=consumer, daemon=True).start()

@app.get("/health")
def health():
    """Health check endpoint"""
    return {"status": "healthy", "service": "classifier"}

