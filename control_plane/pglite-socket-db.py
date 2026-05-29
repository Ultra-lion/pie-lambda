import asyncio
from py_pglite import PGLiteSocketServer



async def run_db():
    server = PGLiteSocketServer(host="127.0.0.1", port=6957, max_connections=100)
    await server.start()

if __name__=="__main__":
    asyncio.run(run_db())
    



