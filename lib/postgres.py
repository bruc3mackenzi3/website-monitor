from datetime import datetime

import psycopg2
from psycopg2.extras import RealDictCursor


class DbClient:

    '''
    PostgreSQL Database client wrapper for connecting and writing monitor stats.
    '''

    def __init__(self, host, db, username, table_name):
        self.table_name = table_name

        with open('auth/pg_pw') as f:
            pg_pw = f.read().rstrip()
        uri = f'postgres://{username}:{pg_pw}@{host}:14539/{db}?sslmode=require'

        db_conn = psycopg2.connect(uri)
        db_conn.autocommit = True
        self.cursor = db_conn.cursor(cursor_factory=RealDictCursor)

    def insert(
            self,
            site_url: str,
            http_status: str,
            response_time_ms: int,
            time: int
    ):
        query = (f"INSERT INTO {self.table_name} "
                 "(site_url, http_status, response_time_ms, time) "
                 "VALUES (%s, %s, %s, %s);")
        self.cursor.execute(
                query,
                (site_url, http_status, response_time_ms,
                 datetime.utcfromtimestamp(time))
        )

    # Utility function for DB table creation
    def _create_table(self):
        self.cursor.execute(f"CREATE TABLE {self.table_name} (id SERIAL PRIMARY KEY, site_url VARCHAR NOT NULL, http_status VARCHAR, response_time_ms INTEGER, time TIMESTAMP NOT NULL);")

# if __name__ == '__main__':
#     DbClient('test_monitor_stats')._create_table()
