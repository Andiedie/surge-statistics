import json
import time
import sqlite3
import surge_api

from dotenv import load_dotenv

load_dotenv()


if __name__ == '__main__':
    api = surge_api.SurgeAPI()

    with sqlite3.connect('surge.db') as conn:
        c = conn.cursor()
        c.execute('CREATE TABLE IF NOT EXISTS requests (id INTEGER PRIMARY KEY, data TEXT)')

        while True:
            merged_requests = {}

            recent_requests = api.recent()['requests']
            for one in recent_requests:
                merged_requests[one['id']] = one

            active_requests = api.active()['requests']
            for one in active_requests:
                rid = one['id']
                if rid not in merged_requests:
                    merged_requests[rid] = one
                r = merged_requests[rid]
                if one['outBytes'] > r['outBytes'] or one['inBytes'] > r['inBytes']:
                    merged_requests[rid] = one

            data = [
                (one['id'], json.dumps(one, ensure_ascii=False, separators=(',', ':')))
                for one in merged_requests.values()
            ]
            c.execute('SELECT COUNT(1) FROM requests')
            before_lines = c.fetchone()[0]
            c.executemany('INSERT OR REPLACE INTO requests VALUES (?, ?)', data)
            c.execute('SELECT COUNT(1) FROM requests')
            after_lines = c.fetchone()[0]
            conn.commit()

            insert_lines = after_lines - before_lines
            update_lines = len(data) - insert_lines
            print(f'insert {insert_lines} rows, update {update_lines} rows, total {after_lines} rows')
            time.sleep(1)
