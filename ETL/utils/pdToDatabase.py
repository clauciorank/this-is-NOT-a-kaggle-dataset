import sqlite3
import pandas as pd


def save_to_sqlite(df, table_name):
    # Create a SQLite connection
    conn = sqlite3.connect('sqlite_database.sqlite')

    # Save the DataFrame to a SQLite table
    df.to_sql(table_name, conn, if_exists='append', index=False, )

    # Close the connection
    conn.close()


def get_data_sqlite(query):
    # Create a SQLite connection
    conn = sqlite3.connect('sqlite_database.sqlite')

    df = pd.read_sql(query, conn)

    conn.close()

    return df
