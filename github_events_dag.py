from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 2, 22),
    "retries": 0,  # ❌ No retries
}

dag = DAG(
    "github_events",
    default_args=default_args,
    schedule_interval=timedelta(minutes=1),
    catchup=False,
)

def fetch_github_events():
    """Fetches GitHub SSE events and sends them in batches to Kafka."""

    import sseclient
    import requests
    import json
    import sys
    import logging
    from kafka import KafkaProducer
    from kafka.errors import KafkaError

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stderr)]
    )

    # Constants
    SSE_URL = "http://github-firehose.libraries.io/events"
    KAFKA_BOOTSTRAP_SERVERS = "kfk-github-kafka-bootstrap.env-g0vgp2.svc.dev.ahq:9092"
    KAFKA_TOPIC = "kfk-t-github-sink"
    KAFKA_USERNAME = "kfk-u-github-ccravens"
    KAFKA_PASSWORD = "vRDFslHaRImo1Xz8WqkOeQa8qmHtXP79"

    BATCH_SIZE = 100  # Adjust batch size as needed
    total_published = 0  # Track the number of published messages

    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="SCRAM-SHA-512",
            sasl_plain_username=KAFKA_USERNAME,
            sasl_plain_password=KAFKA_PASSWORD,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
    except KafkaError as e:
        logging.error(f"❌ Kafka Producer initialization failed: {e}")
        return "Kafka Producer Error"  # Returning error message instead of sys.exit(1)

    try:
        logging.info("🔄 Connecting to GitHub Firehose SSE...")
        response = requests.get(SSE_URL, stream=True, timeout=300, headers={"Accept": "text/event-stream"})

        if response.status_code != 200:
            logging.error(f"❌ Connection failed with status {response.status_code}. Exiting gracefully.")
            return None  # No retry, DAG should not fail

        client = sseclient.SSEClient(response)
        event_batch = []

        for event in client.events():
            try:
                if event.data:
                    json_event = json.loads(event.data)
                    event_batch.append(json_event)

                    if len(event_batch) >= BATCH_SIZE:
                        for record in event_batch:
                            try:
                                producer.send(KAFKA_TOPIC, value=record)
                            except KafkaError as ke:
                                logging.error(f"❌ Kafka send error: {ke}")

                        producer.flush()
                        total_published += len(event_batch)
                        logging.info(f"✅ Published {len(event_batch)} messages (Total: {total_published})")

                        event_batch = []  # Clear batch

            except json.JSONDecodeError as e:
                logging.warning(f"⚠️ JSON Decode Error: {e}")

    except requests.exceptions.ChunkedEncodingError:
        logging.warning("⚠️ SSE Connection closed by the server. Exiting gracefully.")
        return None  # Do not mark the DAG as failed

    except requests.exceptions.ConnectionError:
        logging.warning("⚠️ SSE Connection lost (ConnectionError). Exiting gracefully.")
        return None

    except requests.exceptions.Timeout:
        logging.warning("⚠️ SSE Connection timed out. Exiting gracefully.")
        return None

    except requests.exceptions.RequestException as e:
        logging.error(f"❌ SSE Connection failed: {e}. Exiting gracefully.")
        return None  # Do not mark the DAG as failed

    except Exception as e:
        logging.error(f"⚠️ Unexpected error: {e}. Exiting gracefully.")
        return None  # Do not mark the DAG as failed

    finally:
        logging.info("🛑 Exiting gracefully...")
        producer.close()

    return None  # DAG execution successful

fetch_task = PythonVirtualenvOperator(
    task_id="fetch_github_events",
    python_callable=fetch_github_events,
    requirements=["kafka-python==2.0.3", "requests==2.32.3", "sseclient-py==1.8.0"],
    system_site_packages=False,
    dag=dag,
)

fetch_task
