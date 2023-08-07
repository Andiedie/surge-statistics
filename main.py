import json
import sqlite3
import time
import traceback
import os

from dotenv import load_dotenv

import surge_api

load_dotenv()


if __name__ == '__main__':
    saved_days = os.environ.get('SAVED_DAYS', 14)

    api = surge_api.SurgeAPI()

    with sqlite3.connect('surge.db') as conn:
        c = conn.cursor()
        c.execute('CREATE TABLE IF NOT EXISTS requests (id INTEGER PRIMARY KEY, data TEXT)')

        while True:
            # noinspection PyBroadException
            try:
                merged_requests = {}

                recent_requests = api.recent()['requests']
                for one in recent_requests:
                    merged_requests[one['id']] = one

                active_requests = api.active()['requests']
                for one in active_requests:
                    rid = one['id']
                    if rid not in merged_requests:
                        merged_requests[rid] = one
                    else:
                        r = merged_requests[rid]
                        if one['outBytes'] > r['outBytes'] or one['inBytes'] > r['inBytes']:
                            merged_requests[rid] = one

                data = [
                    (one['id'], json.dumps(one, ensure_ascii=False, separators=(',', ':')))
                    for one in merged_requests.values()
                ]
                c.execute('SELECT COUNT(1) FROM requests')
                before_upsert_lines = c.fetchone()[0]
                c.executemany('INSERT OR REPLACE INTO requests VALUES (?, ?)', data)
                c.execute('SELECT COUNT(1) FROM requests')
                after_upsert_lines = c.fetchone()[0]

                c.execute(
                    '''DELETE FROM requests WHERE json_extract(data, '$.startDate') < ?''',
                    (time.time() - 86400 * saved_days,)
                )
                c.execute('SELECT COUNT(1) FROM requests')
                after_delete_lines = c.fetchone()[0]

                conn.commit()

                insert_lines = after_upsert_lines - before_upsert_lines
                update_lines = len(data) - insert_lines
                delete_lines = after_upsert_lines - after_delete_lines
                print(f'insert {insert_lines}, '
                      f'update {update_lines}, '
                      f'delete {delete_lines}, '
                      f'total {after_delete_lines} rows')
            except Exception:
                traceback.print_exc()
            time.sleep(1)
