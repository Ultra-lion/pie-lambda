import asyncio
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

# Initialize a connection pool for your application
# Replace with your actual connection string
pool = None

async def create_pool():
        global pool
        # 1. Back to clean string
        conn_str = "host=127.0.0.1 port=6957 user=postgres dbname=postgres sslmode=disable"
        
        # 2. Add the configuration rule
        async def configure_conn(conn):
            conn.prepare_threshold = None
        
        # 3. Pass it to the pool
        pool = AsyncConnectionPool(
            conninfo=conn_str,
            kwargs={"row_factory": dict_row}, 
            configure=configure_conn, # Added this
            min_size=1,
            max_size=5,
            open=False
        )
        await pool.open()
        await pool.wait()

async def execute_query(query_string: str, params: tuple = None):
    """
    Executes a SQL query asynchronously and returns all fetched rows.
    """
    global pool
    # Ensure the pool is opened before using it
    if not pool._opened:
        await pool.open()

    # Acquire a connection from the pool
    async with pool.connection() as conn:
        # Open a cursor to execute the command
        async with conn.cursor() as cur:
            await cur.execute(query_string, params)
            
            # Return results if the query returns rows (like SELECT)
            if cur.pgresult and cur.pgresult.ntuples > 0:
                return await cur.fetchall()
            
            # Return None or row count for non-selecting queries (INSERT/UPDATE)
            return cur.rowcount


async def main():
    await create_pool()
    print("pool created")

    update_sql = "update requests set status = 'pending' where request_id='5aa39e70-bfe2-4e5f-8bbc-d16abbbda440'"
    res = await execute_query(update_sql)

    sql_select = "SELECT * from requests;"
    res = await execute_query(sql_select)
    print("Result:", res)
    await pool.close()

# Run the async loop
asyncio.run(main())