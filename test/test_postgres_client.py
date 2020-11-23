from datetime import datetime
import json
import time

from lib.postgres import DbClient

config = json.load(open('config.json'))


class TestDbClient:
    test_table = 'test_monitor_stats'

    # TODO: Test connection to PostgreSQL DB

    def test_insert(self):
        db_client = DbClient(
                config['postgres']['host'],
                config['postgres']['db'],
                config['postgres']['username'],
                self.test_table
        )
        test_url = "http://testwebsite.com"
        http_status = '200'
        response_time = 123
        test_time = int(time.time())

        db_client.insert(
                test_url,
                http_status,
                response_time,
                test_time
        )

        db_client.cursor.execute(
                (f'SELECT * FROM {self.test_table} '
                'WHERE site_url = %s AND http_status = %s'),
                (test_url, http_status)
        )
        results = db_client.cursor.fetchall()

        # Cleanup DB writes
        db_client.cursor.execute(
                f'DELETE FROM {self.test_table} WHERE site_url = %s;',
                (test_url,)
        )

        assert len(results) == 1
        assert results[0]['site_url'] == test_url
        assert results[0]['http_status'] == http_status
        assert results[0]['response_time_ms'] == response_time
        assert results[0]['time'] == datetime.utcfromtimestamp(test_time)
