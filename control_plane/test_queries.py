import sqlite3


def run_query(query):
    db_name="pie_lambda.db"
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    cursor.execute(query)

    # 1. Save changes (crucial for DELETE, INSERT, UPDATE)
    conn.commit()

    # 2. Only try to print rows if it was a SELECT query
    if cursor.description:
        for row in cursor.fetchall():
            print(row)

    conn.close()


run_query("select count(*) from containers;")
# run_query("delete  from containers;")