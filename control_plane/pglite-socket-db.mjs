import { PGlite } from '@electric-sql/pglite';
import { PGLiteSocketServer } from '@electric-sql/pglite-socket';
import { readFile } from 'node:fs/promises';

async function start() {
  let dbPath = 'memory://';

  try {
    const configData = await readFile(new URL('./config.json', import.meta.url), 'utf-8');
    const config = JSON.parse(configData);
    if (config.db_type === 'disk') {
      dbPath = config.db_path || './pgdata';
    }
  } catch (err) {
    // Default to in-memory if config file is missing or invalid
  }

  const db = new PGlite(dbPath);

  const server = new PGLiteSocketServer({
    db,
    host: '0.0.0.0',
    port: 6957,
    maxConnections: 100
  });

  await server.start();
  console.log(`PGlite (${dbPath === 'memory://' ? 'IN-MEMORY' : 'DISK'}) server listening on 0.0.0.0:6957`);
  // Basic logging to see pulses
  db.listen('connect', () => console.log('Client connected to PGlite'));
}

start().catch((err) => {
  console.error('Failed to start PGlite server:', err);
  process.exit(1);
});
