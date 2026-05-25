import sqlite3


def run_query(query):
    db_name="pie_lambda.db"
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    cursor.execute(query)
    for row in cursor.fetchall():
        print(row)

    conn.close()


run_query("select * from containers;")