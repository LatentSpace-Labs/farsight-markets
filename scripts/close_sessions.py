"""Mark any dangling (never-ended) sessions as ended_at=now.

Useful after a hard kill of the runner, where stop() didn't get to run.
"""

from farsight.markets.store import LocalStore


def main():
    store = LocalStore()
    conn = store._get_conn()
    cur = conn.execute(
        "UPDATE sessions SET ended_at = datetime('now') WHERE ended_at IS NULL"
    )
    conn.commit()
    print(f"cleared {cur.rowcount} dangling session(s)")
    store.close()


if __name__ == "__main__":
    main()
