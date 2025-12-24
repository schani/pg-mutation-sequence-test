import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest'
import { Pool, PoolClient } from 'pg'
import { createPool, setupSchema, insertTestRow } from '../src/db'

/**
 * Performance benchmarks comparing mutation throughput:
 * 1. Without locking (incorrect but fast)
 * 2. With locking on `rows` table
 * 3. With locking on `mirror` table
 *
 * Configuration: 5 concurrent writers, 1000 mutations each = 5000 total mutations
 */

const NUM_WRITERS = 5
const MUTATIONS_PER_WRITER = 1000
const TOTAL_MUTATIONS = NUM_WRITERS * MUTATIONS_PER_WRITER

interface BenchmarkResult {
  name: string
  totalTimeMs: number
  mutationsPerSecond: number
  avgLatencyMs: number
}

async function runBenchmark(
  pool: Pool,
  rowId: number,
  name: string,
  mutateFn: (client: PoolClient, rowId: number, value: number) => Promise<void>
): Promise<BenchmarkResult> {
  const startTime = Date.now()

  // Create writer tasks
  const writerPromises: Promise<void>[] = []

  for (let writer = 0; writer < NUM_WRITERS; writer++) {
    writerPromises.push(
      (async () => {
        const client = await pool.connect()
        try {
          for (let i = 0; i < MUTATIONS_PER_WRITER; i++) {
            const value = writer * MUTATIONS_PER_WRITER + i
            await mutateFn(client, rowId, value)
          }
        } finally {
          client.release()
        }
      })()
    )
  }

  await Promise.all(writerPromises)

  const totalTimeMs = Date.now() - startTime
  const mutationsPerSecond = (TOTAL_MUTATIONS / totalTimeMs) * 1000
  const avgLatencyMs = totalTimeMs / TOTAL_MUTATIONS

  return {
    name,
    totalTimeMs,
    mutationsPerSecond,
    avgLatencyMs,
  }
}

// Mutation functions for each strategy
async function mutateNoLocking(client: PoolClient, rowId: number, value: number): Promise<void> {
  await client.query('BEGIN')
  await client.query(
    'INSERT INTO mutations (row_id, new_value) VALUES ($1, $2)',
    [rowId, value]
  )
  await client.query(
    'UPDATE mirror SET value = $1 WHERE row_id = $2',
    [value, rowId]
  )
  await client.query('COMMIT')
}

async function mutateLockRows(client: PoolClient, rowId: number, value: number): Promise<void> {
  await client.query('BEGIN')
  await client.query('SELECT 1 FROM rows WHERE id = $1 FOR UPDATE', [rowId])
  await client.query(
    'INSERT INTO mutations (row_id, new_value) VALUES ($1, $2)',
    [rowId, value]
  )
  await client.query(
    'UPDATE mirror SET value = $1 WHERE row_id = $2',
    [value, rowId]
  )
  await client.query('COMMIT')
}

async function mutateLockMirror(client: PoolClient, rowId: number, value: number): Promise<void> {
  await client.query('BEGIN')
  await client.query('SELECT 1 FROM mirror WHERE row_id = $1 FOR UPDATE', [rowId])
  await client.query(
    'INSERT INTO mutations (row_id, new_value) VALUES ($1, $2)',
    [rowId, value]
  )
  await client.query(
    'UPDATE mirror SET value = $1 WHERE row_id = $2',
    [value, rowId]
  )
  await client.query('COMMIT')
}

describe('Performance Benchmarks', () => {
  let pool: Pool

  beforeAll(async () => {
    pool = createPool()
    await setupSchema(pool)
  })

  afterAll(async () => {
    await pool.end()
  })

  beforeEach(async () => {
    await pool.query('TRUNCATE mutations, mirror, rows RESTART IDENTITY CASCADE')
    await pool.query('ALTER SEQUENCE mutations_seq RESTART WITH 1')
  })

  it(`benchmark: ${NUM_WRITERS} writers × ${MUTATIONS_PER_WRITER} mutations = ${TOTAL_MUTATIONS} total`, async () => {
    const results: BenchmarkResult[] = []

    // Test 1: No locking (fast but incorrect)
    {
      const rowId = await insertTestRow(pool, 'no-lock-bench')
      const result = await runBenchmark(pool, rowId, 'No locking (incorrect)', mutateNoLocking)
      results.push(result)

      // Verify it's actually incorrect - sequence order likely differs from final state
      const mutations = await pool.query(
        'SELECT seq, new_value FROM mutations WHERE row_id = $1 ORDER BY seq',
        [rowId]
      )
      const mirror = await pool.query('SELECT value FROM mirror WHERE row_id = $1', [rowId])
      const lastSeqValue = mutations.rows[mutations.rows.length - 1]?.new_value
      const mirrorValue = mirror.rows[0]?.value

      console.log(`  No locking: last seq value = ${lastSeqValue}, mirror = ${mirrorValue}, match = ${lastSeqValue === mirrorValue}`)
    }

    // Reset for next test
    await pool.query('TRUNCATE mutations RESTART IDENTITY CASCADE')
    await pool.query('ALTER SEQUENCE mutations_seq RESTART WITH 1')

    // Test 2: Lock rows table
    {
      const rowId = await insertTestRow(pool, 'lock-rows-bench')
      const result = await runBenchmark(pool, rowId, 'Lock rows table', mutateLockRows)
      results.push(result)

      // Verify correctness
      const mutations = await pool.query(
        'SELECT seq, new_value FROM mutations WHERE row_id = $1 ORDER BY seq',
        [rowId]
      )
      const mirror = await pool.query('SELECT value FROM mirror WHERE row_id = $1', [rowId])
      const lastSeqValue = mutations.rows[mutations.rows.length - 1]?.new_value
      const mirrorValue = mirror.rows[0]?.value

      expect(lastSeqValue).toBe(mirrorValue)
      console.log(`  Lock rows: last seq value = ${lastSeqValue}, mirror = ${mirrorValue}, match = ${lastSeqValue === mirrorValue}`)
    }

    // Reset for next test
    await pool.query('TRUNCATE mutations RESTART IDENTITY CASCADE')
    await pool.query('ALTER SEQUENCE mutations_seq RESTART WITH 1')

    // Test 3: Lock mirror table
    {
      const rowId = await insertTestRow(pool, 'lock-mirror-bench')
      const result = await runBenchmark(pool, rowId, 'Lock mirror table', mutateLockMirror)
      results.push(result)

      // Verify correctness
      const mutations = await pool.query(
        'SELECT seq, new_value FROM mutations WHERE row_id = $1 ORDER BY seq',
        [rowId]
      )
      const mirror = await pool.query('SELECT value FROM mirror WHERE row_id = $1', [rowId])
      const lastSeqValue = mutations.rows[mutations.rows.length - 1]?.new_value
      const mirrorValue = mirror.rows[0]?.value

      expect(lastSeqValue).toBe(mirrorValue)
      console.log(`  Lock mirror: last seq value = ${lastSeqValue}, mirror = ${mirrorValue}, match = ${lastSeqValue === mirrorValue}`)
    }

    // Print results table
    console.log('\n' + '='.repeat(80))
    console.log('BENCHMARK RESULTS')
    console.log('='.repeat(80))
    console.log(`Configuration: ${NUM_WRITERS} concurrent writers, ${MUTATIONS_PER_WRITER} mutations each`)
    console.log(`Total mutations: ${TOTAL_MUTATIONS}`)
    console.log('-'.repeat(80))
    console.log(
      'Strategy'.padEnd(25) +
      'Total Time'.padEnd(15) +
      'Throughput'.padEnd(20) +
      'Avg Latency'
    )
    console.log('-'.repeat(80))

    for (const r of results) {
      console.log(
        r.name.padEnd(25) +
        `${r.totalTimeMs.toFixed(0)} ms`.padEnd(15) +
        `${r.mutationsPerSecond.toFixed(0)} mut/sec`.padEnd(20) +
        `${r.avgLatencyMs.toFixed(3)} ms`
      )
    }

    console.log('-'.repeat(80))

    // Calculate overhead
    const noLock = results[0]
    const lockRows = results[1]
    const lockMirror = results[2]

    const rowsOverhead = ((lockRows.totalTimeMs / noLock.totalTimeMs) - 1) * 100
    const mirrorOverhead = ((lockMirror.totalTimeMs / noLock.totalTimeMs) - 1) * 100
    const mirrorVsRows = ((lockMirror.totalTimeMs / lockRows.totalTimeMs) - 1) * 100

    console.log(`Lock rows overhead vs no-lock:    ${rowsOverhead > 0 ? '+' : ''}${rowsOverhead.toFixed(1)}%`)
    console.log(`Lock mirror overhead vs no-lock:  ${mirrorOverhead > 0 ? '+' : ''}${mirrorOverhead.toFixed(1)}%`)
    console.log(`Lock mirror vs lock rows:         ${mirrorVsRows > 0 ? '+' : ''}${mirrorVsRows.toFixed(1)}%`)
    console.log('='.repeat(80))
  }, 120000) // 2 minute timeout

  it('benchmark with multiple independent rows (parallel writes)', async () => {
    // Each writer gets its own row - no contention between writers
    const results: BenchmarkResult[] = []

    // Create rows for each writer upfront
    const rowIds: number[] = []
    for (let i = 0; i < NUM_WRITERS; i++) {
      rowIds.push(await insertTestRow(pool, `parallel-row-${i}`))
    }

    async function runParallelBenchmark(
      name: string,
      mutateFn: (client: PoolClient, rowId: number, value: number) => Promise<void>
    ): Promise<BenchmarkResult> {
      const startTime = Date.now()

      const writerPromises = rowIds.map((rowId, writer) =>
        (async () => {
          const client = await pool.connect()
          try {
            for (let i = 0; i < MUTATIONS_PER_WRITER; i++) {
              await mutateFn(client, rowId, i)
            }
          } finally {
            client.release()
          }
        })()
      )

      await Promise.all(writerPromises)

      const totalTimeMs = Date.now() - startTime
      return {
        name,
        totalTimeMs,
        mutationsPerSecond: (TOTAL_MUTATIONS / totalTimeMs) * 1000,
        avgLatencyMs: totalTimeMs / TOTAL_MUTATIONS,
      }
    }

    // Reset mutations between tests
    const resetMutations = async () => {
      await pool.query('TRUNCATE mutations RESTART IDENTITY CASCADE')
      await pool.query('ALTER SEQUENCE mutations_seq RESTART WITH 1')
      for (const rowId of rowIds) {
        await pool.query('UPDATE mirror SET value = 0 WHERE row_id = $1', [rowId])
      }
    }

    results.push(await runParallelBenchmark('No locking (parallel)', mutateNoLocking))
    await resetMutations()

    results.push(await runParallelBenchmark('Lock rows (parallel)', mutateLockRows))
    await resetMutations()

    results.push(await runParallelBenchmark('Lock mirror (parallel)', mutateLockMirror))

    // Print results
    console.log('\n' + '='.repeat(80))
    console.log('PARALLEL BENCHMARK (each writer has its own row - no lock contention)')
    console.log('='.repeat(80))
    console.log(`Configuration: ${NUM_WRITERS} writers, ${MUTATIONS_PER_WRITER} mutations each, ${NUM_WRITERS} separate rows`)
    console.log('-'.repeat(80))
    console.log(
      'Strategy'.padEnd(30) +
      'Total Time'.padEnd(15) +
      'Throughput'.padEnd(20) +
      'Avg Latency'
    )
    console.log('-'.repeat(80))

    for (const r of results) {
      console.log(
        r.name.padEnd(30) +
        `${r.totalTimeMs.toFixed(0)} ms`.padEnd(15) +
        `${r.mutationsPerSecond.toFixed(0)} mut/sec`.padEnd(20) +
        `${r.avgLatencyMs.toFixed(3)} ms`
      )
    }
    console.log('='.repeat(80))
    console.log('Note: With separate rows, locking overhead should be minimal (no contention)')
  }, 120000)
})
