
from datetime import datetime
from math import isfinite


# Required event fields
REQUIRED_FIELDS = {
    "event_id",
    "country",
    "year",
    "gdp",
    "family",
    "health",
    "freedom",
    "generosity",
    "corruption",
    "actual_happiness_score",
    "event_time"
}

# Fields that must contain numeric values
NUMERIC_FIELDS = {
    "gdp",
    "family",
    "health",
    "freedom",
    "generosity",
    "corruption",
    "actual_happiness_score"
}
# Predictor fields that may contain missing values.
# The ML pipeline will handle them using its median imputer.
NULLABLE_NUMERIC_FIELDS = {
    "gdp",
    "family",
    "health",
    "freedom",
    "generosity",
    "corruption"
}
# Years supported by the trained ML model
SUPPORTED_YEARS = {2018, 2019}



def validate_event(event):
    """
    Validate an incoming Kafka event against the event contract.

    Returns:
        (status, errors)
    """

    if not isinstance(event, dict):
        return "INVALID_SCHEMA", ["Event must be a JSON object."]

    schema_errors = []
    value_errors = []

    # Check required fields
    missing_fields = REQUIRED_FIELDS - event.keys()

    if missing_fields:
        schema_errors.append(
            f"Missing required fields: {sorted(missing_fields)}"
        )

    # Structural and type validation
    if "event_id" in event and (
        not isinstance(event["event_id"], str)
        or not event["event_id"].strip()
    ):
        schema_errors.append("event_id must be a non-empty string.")

    if "country" in event and (
        not isinstance(event["country"], str)
        or not event["country"].strip()
    ):
        schema_errors.append("country must be a non-empty string.")

    if "year" in event:
        year = event["year"]

        if isinstance(year, bool) or not isinstance(year, int):
            schema_errors.append("year must be an integer.")
        elif year not in SUPPORTED_YEARS:
            value_errors.append(
                f"year must be one of {sorted(SUPPORTED_YEARS)}."
            )
            
    # Numeric validation
    for field in NUMERIC_FIELDS.intersection(event.keys()):
        value = event[field]

        # Missing predictor values are allowed.
        if value is None and field in NULLABLE_NUMERIC_FIELDS:
            continue

        if isinstance(value, bool) or not isinstance(value, (int, float)):
            schema_errors.append(f"{field} must be numeric.")
        elif not isfinite(value):
            value_errors.append(
                f"{field} must contain a finite number."
            )

    # Timestamp validation
    if "event_time" in event:
        timestamp = event["event_time"]

        if not isinstance(timestamp, str):
            schema_errors.append("event_time must be a string.")
        else:
            try:
                datetime.fromisoformat(
                    timestamp.replace("Z", "+00:00")
                )
            except ValueError:
                value_errors.append(
                    "event_time must be a valid ISO 8601 timestamp."
                )

    if schema_errors:
        return "INVALID_SCHEMA", schema_errors + value_errors

    if value_errors:
        return "INVALID_VALUES", value_errors

    return "VALID", []
