
import json
import logging
from pathlib import Path
from datetime import datetime

import joblib
import pandas as pd

from kafka import KafkaConsumer
from kafka.structs import TopicPartition, OffsetAndMetadata

from streaming.db_operations import (
    insert_raw_event,
    update_raw_event,
    get_country_id,
    get_date_id,
    save_prediction
)

from streaming.event_contract import validate_event


# --------------------------------------------------
# PROJECT CONFIGURATION
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MODEL_PATH = PROJECT_ROOT / "models" / "model.pkl"

KAFKA_TOPIC = "happiness-events"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_GROUP_ID = "happiness-ml-team"

FEATURE_COLUMNS = [
    "gdp",
    "social_support",
    "health",
    "freedom",
    "generosity",
    "corruption"
]


# --------------------------------------------------
# MODEL LOADING
# --------------------------------------------------

def load_model():
    if not MODEL_PATH.exists():
        raise FileNotFoundError(
            f"Model file not found: {MODEL_PATH}"
        )

    model = joblib.load(MODEL_PATH)

    print("ML model loaded successfully.")

    return model


# --------------------------------------------------
# EVENT PROCESSING
# --------------------------------------------------

def process_message(message, model):

    raw_message = message.value.decode("utf-8", errors="replace")

    # 1. Store original Kafka message in PostgreSQL

    raw_event_id, inserted = insert_raw_event(
        raw_message,
        message.topic,
        message.partition,
        message.offset
    )

    if inserted:
        print(f"\nNew event stored: {raw_event_id}")
    else:
        print(f"\nExisting event found: {raw_event_id}. Reprocessing safely.")

    # 2. Parse JSON

    try:
        event = json.loads(raw_message)

    except json.JSONDecodeError as error:

        update_raw_event(
            raw_event_id,
            "INVALID_SCHEMA",
            [f"Invalid JSON: {str(error)}"]
        )

        print("Event rejected: INVALID_SCHEMA")

        return "INVALID_SCHEMA"

    # 3. Validate event contract

    status, errors = validate_event(event)

    if status != "VALID":

        update_raw_event(
            raw_event_id,
            status,
            errors,
            event
        )

        print(f"Event rejected: {status}")
        print(f"Validation errors: {errors}")

        return status

    # 4. Prepare model features

    features = {
        "gdp": event["gdp"],
        "social_support": event["family"],
        "health": event["health"],
        "freedom": event["freedom"],
        "generosity": event["generosity"],
        "corruption": event["corruption"]
    }

    input_data = pd.DataFrame(
        [features],
        columns=FEATURE_COLUMNS
    )

    # 5. Generate prediction and save results

    try:

        prediction = float(model.predict(input_data)[0])

        actual_score = float(event["actual_happiness_score"])

        event_time = datetime.fromisoformat(
            event["event_time"].replace("Z", "+00:00")
        )

        country_id = get_country_id(event["country"])

        date_id = get_date_id(event_time)

        save_prediction(
            raw_event_id,
            country_id,
            date_id,
            actual_score,
            prediction
        )

        # 6. Mark event as successfully processed

        update_raw_event(
            raw_event_id,
            "VALID",
            None,
            event
        )

        print(f"Country: {event['country']}")
        print(f"Actual score: {actual_score:.4f}")
        print(f"Predicted score: {prediction:.4f}")
        print(f"Prediction error: {actual_score - prediction:.4f}")
        print("Processing status: VALID")

        return "VALID"

    except Exception as error:

        update_raw_event(
            raw_event_id,
            "PREDICTION_ERROR",
            [str(error)],
            event
        )

        print(f"Prediction failed: {error}")

        return "PREDICTION_ERROR"


# --------------------------------------------------
# KAFKA CONSUMER
# --------------------------------------------------

def main():

    model = load_model()

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        max_poll_records=1
    )

    print("\nKafka consumer started.")
    print(f"Topic: {KAFKA_TOPIC}")
    print(f"Consumer group: {KAFKA_GROUP_ID}")
    print("Waiting for messages...\n")

    try:

        for message in consumer:

            status = process_message(message, model)

            # Commit only after the database operations finish

            consumer.commit({
                TopicPartition(
                    message.topic,
                    message.partition
                ): OffsetAndMetadata(
                    message.offset + 1,
                    ""
                )
            })

            print(f"Kafka offset committed: {message.offset}")
            print(f"Final status: {status}")
            print("-" * 50)

    except KeyboardInterrupt:

        print("\nConsumer stopped by user.")

    finally:

        consumer.close()
        print("Kafka consumer connection closed.")


if __name__ == "__main__":
    main()
