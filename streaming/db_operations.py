
from datetime import datetime

from psycopg.types.json import Jsonb

from streaming.db_connection import get_connection

def insert_raw_event(raw_message, topic, partition, offset):
    """
    Store the original Kafka message before validation.

    Returns:
        (raw_event_id, inserted)
    """

    with get_connection() as connection:
        with connection.cursor() as cursor:

            cursor.execute(
                """
                INSERT INTO raw_happiness_events (
                    raw_message,
                    kafka_topic,
                    kafka_partition,
                    kafka_offset
                )
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (
                    kafka_topic,
                    kafka_partition,
                    kafka_offset
                ) DO NOTHING
                RETURNING raw_event_id;
                """,
                (raw_message, topic, partition, offset)
            )

            result = cursor.fetchone()

            if result:
                return result[0], True

            # The Kafka message was already stored.
            cursor.execute(
                """
                SELECT raw_event_id
                FROM raw_happiness_events
                WHERE kafka_topic = %s
                  AND kafka_partition = %s
                  AND kafka_offset = %s;
                """,
                (topic, partition, offset)
            )

            return cursor.fetchone()[0], False


def update_raw_event(raw_event_id, status, errors=None, event=None):
    """
    Update the validation status and available event metadata.
    """

    event = event or {}

    event_time = None

    if isinstance(event.get("event_time"), str):
        try:
            event_time = datetime.fromisoformat(
                event["event_time"].replace("Z", "+00:00")
            )
        except ValueError:
            pass

    with get_connection() as connection:
        with connection.cursor() as cursor:

            cursor.execute(
                """
                UPDATE raw_happiness_events
                SET
                    event_id = %s,
                    country = %s,
                    observation_year = %s,
                    event_time = %s,
                    processing_status = %s,
                    validation_errors = %s
                WHERE raw_event_id = %s;
                """,
                (
                    event.get("event_id"),
                    event.get("country"),
                    event.get("year"),
                    event_time,
                    status,
                    Jsonb(errors) if errors else None,
                    raw_event_id
                )
            )


def get_country_id(country_name):
    """
    Insert a country if necessary and return its dimension ID.
    """

    with get_connection() as connection:
        with connection.cursor() as cursor:

            cursor.execute(
                """
                INSERT INTO dim_country (country_name)
                VALUES (%s)
                ON CONFLICT (country_name)
                DO UPDATE SET country_name = EXCLUDED.country_name
                RETURNING country_id;
                """,
                (country_name,)
            )

            return cursor.fetchone()[0]


def get_date_id(event_time):
    """
    Insert an event date if necessary and return its dimension ID.
    """

    event_date = event_time.date()

    with get_connection() as connection:
        with connection.cursor() as cursor:

            cursor.execute(
                """
                INSERT INTO dim_date (
                    full_date,
                    day,
                    month,
                    year,
                    quarter
                )
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (full_date)
                DO UPDATE SET full_date = EXCLUDED.full_date
                RETURNING date_id;
                """,
                (
                    event_date,
                    event_date.day,
                    event_date.month,
                    event_date.year,
                    (event_date.month - 1) // 3 + 1
                )
            )

            return cursor.fetchone()[0]


def save_prediction(
    raw_event_id,
    country_id,
    date_id,
    actual_score,
    predicted_score
):
    """
    Store the model prediction and its signed error.

    prediction_error = actual score - predicted score.
    """

    prediction_error = actual_score - predicted_score

    with get_connection() as connection:
        with connection.cursor() as cursor:

            cursor.execute(
                """
                INSERT INTO fact_predictions (
                    raw_event_id,
                    country_id,
                    date_id,
                    actual_score,
                    predicted_score,
                    prediction_error
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (raw_event_id) DO NOTHING;
                """,
                (
                    raw_event_id,
                    country_id,
                    date_id,
                    actual_score,
                    predicted_score,
                    prediction_error
                )
            )
