
import os
from pathlib import Path

import psycopg
from dotenv import load_dotenv


# Locate the project root and load environment variables
PROJECT_ROOT = Path(__file__).resolve().parent.parent
ENV_FILE = PROJECT_ROOT / ".env"

load_dotenv(ENV_FILE)


def get_connection():
    """
    Create and return a PostgreSQL database connection.
    """

    required_variables = [
        "POSTGRES_DB",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD"
    ]

    missing_variables = [
        variable
        for variable in required_variables
        if not os.getenv(variable)
    ]

    if missing_variables:
        raise EnvironmentError(
            f"Missing environment variables: {missing_variables}"
        )

    connection = psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5433")),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        connect_timeout=5
    )

    return connection


if __name__ == "__main__":

    try:
        with get_connection() as connection:
            with connection.cursor() as cursor:

                cursor.execute("""
                    SELECT
                        current_database(),
                        current_user,
                        version();
                """)

                result = cursor.fetchone()

                print("\nPOSTGRESQL CONNECTION SUCCESSFUL")
                print("---------------------------------")
                print(f"Database: {result[0]}")
                print(f"User: {result[1]}")
                print(f"Version: {result[2]}")

    except Exception as error:
        print("\nPOSTGRESQL CONNECTION FAILED")
        print(f"Error: {error}")
