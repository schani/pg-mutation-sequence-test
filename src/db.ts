import { Pool, PoolClient } from 'pg'

export function createPool(): Pool {
  return new Pool({
    host: process.env.PGHOST || 'localhost',
    port: parseInt(process.env.PGPORT || '5432'),
    user: process.env.PGUSER || 'postgres',
    password: process.env.PGPASSWORD || 'postgres',
    database: process.env.PGDATABASE || 'mutation_test',
  })
}

export async function setupSchema(pool: Pool): Promise<void> {
  await pool.query(`
    -- Drop existing objects if they exist
    DROP TABLE IF EXISTS mutations CASCADE;
    DROP TABLE IF EXISTS mirror CASCADE;
    DROP TABLE IF EXISTS rows CASCADE;
    DROP SEQUENCE IF EXISTS mutations_seq CASCADE;

    -- The rows table: our bookkeeping/entity table
    CREATE TABLE rows (
      id SERIAL PRIMARY KEY,
      name TEXT NOT NULL
    );

    -- The mirror table: stores the current state of each row
    CREATE TABLE mirror (
      row_id INTEGER PRIMARY KEY REFERENCES rows(id),
      value INTEGER NOT NULL DEFAULT 0
    );

    -- Sequence for mutation ordering
    CREATE SEQUENCE mutations_seq;

    -- The mutations table: stores all mutations with sequence numbers
    CREATE TABLE mutations (
      seq BIGINT PRIMARY KEY DEFAULT nextval('mutations_seq'),
      row_id INTEGER NOT NULL REFERENCES rows(id),
      new_value INTEGER NOT NULL,
      created_at TIMESTAMPTZ DEFAULT now()
    );

    -- Create index for ordering mutations by sequence
    CREATE INDEX mutations_row_seq_idx ON mutations(row_id, seq);
  `)
}

export async function insertTestRow(pool: Pool, name: string): Promise<number> {
  const result = await pool.query(
    'INSERT INTO rows (name) VALUES ($1) RETURNING id',
    [name]
  )
  const rowId = result.rows[0].id

  // Initialize mirror row
  await pool.query(
    'INSERT INTO mirror (row_id, value) VALUES ($1, 0)',
    [rowId]
  )

  return rowId
}

export interface MutationRecord {
  seq: number
  row_id: number
  new_value: number
}

export async function getMutationsInOrder(pool: Pool, rowId: number): Promise<MutationRecord[]> {
  const result = await pool.query(
    'SELECT seq, row_id, new_value FROM mutations WHERE row_id = $1 ORDER BY seq',
    [rowId]
  )
  return result.rows.map(r => ({
    seq: parseInt(r.seq),
    row_id: r.row_id,
    new_value: r.new_value,
  }))
}

export async function getMirrorValue(pool: Pool, rowId: number): Promise<number> {
  const result = await pool.query(
    'SELECT value FROM mirror WHERE row_id = $1',
    [rowId]
  )
  return result.rows[0]?.value
}

/**
 * Perform a mutation WITHOUT locking - demonstrates the race condition.
 * The sequence number is assigned at INSERT time, not at COMMIT time.
 */
export async function mutateWithoutLocking(
  client: PoolClient,
  rowId: number,
  newValue: number,
  delayBeforeCommitMs?: number
): Promise<number> {
  await client.query('BEGIN')

  // INSERT gets sequence number immediately via nextval()
  const result = await client.query(
    'INSERT INTO mutations (row_id, new_value) VALUES ($1, $2) RETURNING seq',
    [rowId, newValue]
  )
  const seq = parseInt(result.rows[0].seq)

  // Update the mirror
  await client.query(
    'UPDATE mirror SET value = $1 WHERE row_id = $2',
    [newValue, rowId]
  )

  // Optional delay to simulate slow transaction
  if (delayBeforeCommitMs) {
    await new Promise(resolve => setTimeout(resolve, delayBeforeCommitMs))
  }

  await client.query('COMMIT')

  return seq
}

/**
 * Perform a mutation WITH row locking - ensures sequence order matches commit order.
 * By locking the row first, we serialize access and ensure the sequence
 * number is assigned only after acquiring the lock.
 */
export async function mutateWithLocking(
  client: PoolClient,
  rowId: number,
  newValue: number,
  delayBeforeCommitMs?: number
): Promise<number> {
  await client.query('BEGIN')

  // Lock the row first - this serializes concurrent transactions
  await client.query(
    'SELECT 1 FROM rows WHERE id = $1 FOR UPDATE',
    [rowId]
  )

  // Now get sequence number - only happens after lock acquired
  const result = await client.query(
    'INSERT INTO mutations (row_id, new_value) VALUES ($1, $2) RETURNING seq',
    [rowId, newValue]
  )
  const seq = parseInt(result.rows[0].seq)

  // Update the mirror
  await client.query(
    'UPDATE mirror SET value = $1 WHERE row_id = $2',
    [newValue, rowId]
  )

  // Optional delay to simulate slow transaction
  if (delayBeforeCommitMs) {
    await new Promise(resolve => setTimeout(resolve, delayBeforeCommitMs))
  }

  await client.query('COMMIT')

  return seq
}
