import { PGlite } from '@electric-sql/pglite';
import { PGLiteSocketServer } from '@electric-sql/pglite-socket';

async function start() {
  // Use memory:// for zero-persistence, high-speed RAM storage
  const db = new PGlite('memory://');

  const server = new PGLiteSocketServer({
    db,
    host: '0.0.0.0',
    port: 6957
  });

  await server.start();
  console.log('PGlite IN-MEMORY server listening on 0.0.0.0:6957');
}

start().catch((err) => {
  console.error('Failed to start PGlite server:', err);
  process.exit(1);
});
