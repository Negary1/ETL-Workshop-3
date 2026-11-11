
import argparse
import json
import time
import uuid

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer

from streaming.event_contract import validate_event


# Project configuration
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_PATH = PROJECT_ROOT / "data" / "processed" / "happiness_unified.csv"

KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "happiness-events"

SUPPORTED_YEARS = [2018, 2019]

# CSV-to-event field mapping
FIELD_MAPPING = {
    "gdp": "gdp",
    "social_support": "family",
    "health": "health",
    "freedom": "freedom",
    "generosity": "generosity",
    "corruption": "corruption"
}


def build_event(row):
    """Convert one historical CSV row into a Kafka event."""

    event = {
        "event_id": str(uuid.uuid4()),
        "country": str(row["country"]).strip(),
        "year": int(row["year"]),
        "actual_happiness_score": float(row["happiness_score"]),
        "event_time": datetime.now(timezone.utc).isoformat()
    }

    for source_field, event_field in FIELD_MAPPING.items():
        value = row[source_field]

        # Preserve missing source values as JSON null.
        if pd.isna(value):
            event[event_field] = None
        else:
            event[event_field] = float(value)

    return event


def load_events():
    """Load and prepare historical observations."""

    if not DATA_PATH.exists():
        raise FileNotFoundError(
            f"Dataset not found: {DATA_PATH}"
        )

    df = pd.read_csv(DATA_PATH)

    required_columns = {
        "country",
        "year",
        "happiness_score",
        *FIELD_MAPPING.keys()
    }

    missing_columns = required_columns - set(df.columns)

    if missing_columns:
        raise ValueError(
            f"Missing CSV columns: {sorted(missing_columns)}"
        )

    df = df[df["year"].isin(SUPPORTED_YEARS)].copy()

    if df.empty:
        raise ValueError("No records found for 2018 and 2019.")

    events = [build_event(row) for _, row in df.iterrows()]

    return events


def main():
    parser = argparse.ArgumentParser(
        description="Publish World Happiness events to Kafka."
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Maximum number of events to publish. Use 312 for all."
    )

    parser.add_argument(
        "--delay",
        type=float,
        default=0.5,
        help="Delay in seconds between events."
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and preview events without publishing."
    )

    args = parser.parse_args()

    if args.limit <= 0 or args.delay < 0:
        parser.error("--limit must be positive and --delay cannot be negative.")

    events = load_events()

    print(f"Historical events loaded: {len(events)}")

    selected_events = events[:args.limit]

    # Validate all selected events before publishing.
    for index, event in enumerate(selected_events, start=1):
        status, errors = validate_event(event)

        if status != "VALID":
            raise ValueError(
                f"Event {index} failed validation: {errors}"
            )

    print(f"Events selected: {len(selected_events)}")
    print("Contract validation: PASSED")

    if args.dry_run:
        print("\nDRY RUN - No messages sent to Kafka.")
        print("\nFirst event preview:")
        print(json.dumps(selected_events[0], indent=4))
        return

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(
            value,
            allow_nan=False
        ).encode("utf-8"),
        acks="all",
        retries=5
    )

    try:
        for index, event in enumerate(selected_events, start=1):

            future = producer.send(
                KAFKA_TOPIC,
                key=event["country"],
                value=event
            )

            metadata = future.get(timeout=10)

            print(
                f"[{index}/{len(selected_events)}] "
                f"Country: {event['country']} | "
                f"Partition: {metadata.partition} | "
                f"Offset: {metadata.offset}"
            )

            time.sleep(args.delay)

        producer.flush()

        print("\nPRODUCER COMPLETED")
        print(f"Successfully published: {len(selected_events)}")

    finally:
        producer.close()


if __name__ == "__main__":
    main()

