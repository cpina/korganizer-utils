#!/usr/bin/env python3

import argparse
from pathlib import Path

import pymysql

db_params = {
    'unix_socket': '/run/user/1000/akonadi/mysql.socket',
    'user': '[User]',
    'password': '[Password]',
    'db': 'akonadi'
}

def validate(directory: Path):
    connection = pymysql.connect(**db_params)

    try:
        # Create a cursor object to interact with the MySQL server
        with connection.cursor(pymysql.cursors.DictCursor) as cursor:
            # Define your SQL query
            sql_query = "SELECT * FROM pimitemtable LEFT JOIN parttable ON pimitemtable.id=parttable.pimItemId"

            # Execute the SQL query
            cursor.execute(sql_query)

            # Fetch all the rows
            rows = cursor.fetchall()

            missing = 0

            for row in rows:
                uuid = row["remoteId"]

                if uuid is None:
                    continue

                missing += 1

                if not (directory / uuid.decode("utf-8")).exists():
                    print("Missing:", uuid)
                    print(row["data"])

            print("Total missing:", missing)


    finally:
        # Close the connection to the MySQL server
        connection.close()

def main():
    parser = argparse.ArgumentParser(description="Check that events in the Akonadi database exist in the korganizer directory")
    parser.add_argument("korganizer_vcalendar_directory", help="Directory with the files", type=Path)

    args = parser.parse_args()

    if not args.korganizer_vcalendar_directory.is_dir():
        print(f"The directory {args.directory} does not exist")
        raise SystemExit(3)

    validate(args.korganizer_vcalendar_directory)

if __name__ == "__main__":
    main()