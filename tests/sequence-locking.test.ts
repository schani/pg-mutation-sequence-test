import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { Pool, PoolClient } from 'pg'
import { createPool, setupSchema, insertTestRow, getMutationsInOrder, getMirrorValue } from '../src/db'

/**
 * These tests demonstrate the PostgreSQL sequence ordering problem and its solution.
 *
 * THE PROBLEM:
 * Sequences increment at nextval() time, not at commit time. This means:
 * - Transaction A can get sequence 100
 * - Transaction B can get sequence 101
 * - Transaction B can COMMIT before Transaction A
 *
 * If you replay mutations in sequence order, you get the wrong final state
 * because the sequence order doesn't match the actual commit order.
 *
 * THE SOLUTION:
 * Lock the row with SELECT ... FOR UPDATE before getting the sequence number.
 * This serializes access, ensuring sequence numbers are assigned in commit order.
 */

async function safeRollback(client: PoolClient) {
  try {
    await client.query('ROLLBACK')
  } catch {
    // Ignore - connection may already be closed
  }
}

describe('PostgreSQL Sequence Locking', () => {
  let pool: Pool

  beforeAll(async () => {
    pool = createPool()
    await setupSchema(pool)
  })

  afterAll(async () => {
    await pool.end()
  })

  describe('WITHOUT locking (demonstrates the bug)', () => {
    it('demonstrates that sequence order can diverge from commit order', async () => {
      /**
       * This test demonstrates that without explicit locking, the sequence
       * numbers are assigned at INSERT time, not at COMMIT time.
       *
       * The practical impact: if you're replaying mutations from a log,
       * and two transactions on different rows reference each other
       * (or a downstream system consumes mutations in sequence order),
       * the order seen may not match the actual commit order.
       *
       * We use two separate rows to avoid UPDATE lock contention,
       * which would otherwise serialize the transactions.
       */
      const rowId1 = await insertTestRow(pool, `row1-${Date.now()}`)
      const rowId2 = await insertTestRow(pool, `row2-${Date.now()}`)

      const clientA = await pool.connect()
      const clientB = await pool.connect()

      const events: string[] = []
      let bCommitted = false

      try {
        // Transaction A: gets sequence first, but commits LAST
        const txA = (async () => {
          await clientA.query('BEGIN')
          events.push('A: BEGIN')

          const result = await clientA.query(
            'INSERT INTO mutations (row_id, new_value) VALUES ($1, $2) RETURNING seq',
            [rowId1, 111]
          )
          const seq = parseInt(result.rows[0].seq)
          events.push(`A: got seq ${seq}`)

          await clientA.query('UPDATE mirror SET value = $1 WHERE row_id = $2', [111, rowId1])
          events.push('A: updated mirror for row1')

          // Wait until B has committed
          while (!bCommitted) {
            await new Promise(resolve => setTimeout(resolve, 5))
          }
          events.push('A: B has committed, now committing')

          await clientA.query('COMMIT')
          events.push('A: COMMIT')

          return seq
        })()

        await new Promise(resolve => setTimeout(resolve, 20))

        // Transaction B: gets higher sequence, but commits FIRST
        const txB = (async () => {
          await clientB.query('BEGIN')
          events.push('B: BEGIN')

          const result = await clientB.query(
            'INSERT INTO mutations (row_id, new_value) VALUES ($1, $2) RETURNING seq',
            [rowId2, 222]
          )
          const seq = parseInt(result.rows[0].seq)
          events.push(`B: got seq ${seq}`)

          await clientB.query('UPDATE mirror SET value = $1 WHERE row_id = $2', [222, rowId2])
          events.push('B: updated mirror for row2')

          await clientB.query('COMMIT')
          events.push('B: COMMIT')
          bCommitted = true

          return seq
        })()

        const [seqA, seqB] = await Promise.all([txA, txB])

        console.log('Event order:', events)

        // A got lower sequence but committed AFTER B
        expect(seqA).toBeLessThan(seqB)

        // Check the commit order by looking at pg_stat_activity or xact timestamps
        // We can verify by checking that B's commit happened before A's
        // (In this controlled test, we know this because of our synchronization)

        // The key insight: if a consumer reads mutations in sequence order,
        // they would see: seq1 (row1=111) then seq2 (row2=222)
        // But the actual commit order was: B first, then A
        //
        // This matters for:
        // 1. Cross-row constraints (if row2 depends on row1's state)
        // 2. External consumers that need to see changes in commit order
        // 3. Replication/CDC systems

        const allMutations = await pool.query(
          'SELECT seq, row_id, new_value FROM mutations ORDER BY seq'
        )

        console.log('\nSEQUENCE vs COMMIT ORDER DIVERGENCE:')
        console.log(`  Sequence order: seq${seqA} (row1) → seq${seqB} (row2)`)
        console.log(`  Commit order: B (row2) → A (row1)`)
        console.log('  These do NOT match!')
        console.log('\n  A consumer reading in sequence order would see:')
        console.log(`    1. row1 changed to 111 (seq ${seqA})`)
        console.log(`    2. row2 changed to 222 (seq ${seqB})`)
        console.log('  But the actual commit order was row2 first, then row1\n')

        // Verify the sequence assignment happened before commit finalization
        expect(events.indexOf(`A: got seq ${seqA}`)).toBeLessThan(
          events.indexOf('B: COMMIT')
        )
      } catch (e) {
        await safeRollback(clientA)
        await safeRollback(clientB)
        throw e
      } finally {
        clientA.release()
        clientB.release()
      }
    })
  })

  describe('WITH locking (the fix)', () => {
    it('should guarantee sequence order matches commit order', async () => {
      const rowId = await insertTestRow(pool, `locked-row-${Date.now()}`)

      const clientA = await pool.connect()
      const clientB = await pool.connect()

      const events: string[] = []

      try {
        // Transaction A: acquires lock, gets sequence, takes time to commit
        const txA = (async () => {
          await clientA.query('BEGIN')
          events.push('A: BEGIN')

          // Lock the row first!
          await clientA.query('SELECT 1 FROM rows WHERE id = $1 FOR UPDATE', [rowId])
          events.push('A: acquired lock')

          const result = await clientA.query(
            'INSERT INTO mutations (row_id, new_value) VALUES ($1, $2) RETURNING seq',
            [rowId, 111]
          )
          const seq = parseInt(result.rows[0].seq)
          events.push(`A: got seq ${seq}`)

          await clientA.query('UPDATE mirror SET value = $1 WHERE row_id = $2', [111, rowId])
          events.push('A: updated mirror to 111')

          // Simulate slow transaction
          await new Promise(resolve => setTimeout(resolve, 50))

          await clientA.query('COMMIT')
          events.push('A: COMMIT (releases lock)')

          return seq
        })()

        // Transaction B: tries to get lock, has to wait for A
        await new Promise(resolve => setTimeout(resolve, 10)) // Let A start

        const txB = (async () => {
          await clientB.query('BEGIN')
          events.push('B: BEGIN')

          // This will BLOCK until A commits
          events.push('B: waiting for lock...')
          await clientB.query('SELECT 1 FROM rows WHERE id = $1 FOR UPDATE', [rowId])
          events.push('B: acquired lock (A must have committed)')

          const result = await clientB.query(
            'INSERT INTO mutations (row_id, new_value) VALUES ($1, $2) RETURNING seq',
            [rowId, 222]
          )
          const seq = parseInt(result.rows[0].seq)
          events.push(`B: got seq ${seq}`)

          await clientB.query('UPDATE mirror SET value = $1 WHERE row_id = $2', [222, rowId])
          events.push('B: updated mirror to 222')

          await clientB.query('COMMIT')
          events.push('B: COMMIT')

          return seq
        })()

        const [seqA, seqB] = await Promise.all([txA, txB])

        console.log('Event order:', events)

        // A still gets lower sequence (it started first and got lock first)
        expect(seqA).toBeLessThan(seqB)

        // B commits last now (had to wait for lock), so mirror has B's value
        const mirrorValue = await getMirrorValue(pool, rowId)
        expect(mirrorValue).toBe(222) // B's value, because B committed last

        // The mutations in sequence order
        const mutations = await getMutationsInOrder(pool, rowId)
        expect(mutations.map(m => m.new_value)).toEqual([111, 222])

        // Replaying gives us 222 (last in sequence order)
        // Mirror value is ALSO 222 (last to commit)
        // THEY MATCH! The bug is fixed!
        const replayResult = mutations[mutations.length - 1].new_value
        expect(replayResult).toBe(222)
        expect(mirrorValue).toBe(222)
        expect(replayResult).toBe(mirrorValue) // They match now!

        console.log('\nFIX VERIFIED:')
        console.log(`  Sequence order: ${mutations.map(m => `seq${m.seq}=${m.new_value}`).join(' -> ')}`)
        console.log(`  Replay would produce: ${replayResult}`)
        console.log(`  Actual mirror value: ${mirrorValue}`)
        console.log('  ✓ They match!\n')
      } catch (e) {
        await safeRollback(clientA)
        await safeRollback(clientB)
        throw e
      } finally {
        clientA.release()
        clientB.release()
      }
    })

    it('locking on mirror table instead of rows table also works', async () => {
      const rowId = await insertTestRow(pool, `mirror-lock-${Date.now()}`)

      const clientA = await pool.connect()
      const clientB = await pool.connect()

      try {
        // A locks using mirror table
        const txA = (async () => {
          await clientA.query('BEGIN')
          await clientA.query('SELECT 1 FROM mirror WHERE row_id = $1 FOR UPDATE', [rowId])

          const result = await clientA.query(
            'INSERT INTO mutations (row_id, new_value) VALUES ($1, $2) RETURNING seq',
            [rowId, 111]
          )
          const seq = parseInt(result.rows[0].seq)

          await clientA.query('UPDATE mirror SET value = $1 WHERE row_id = $2', [111, rowId])
          await new Promise(resolve => setTimeout(resolve, 50))
          await clientA.query('COMMIT')
          return seq
        })()

        await new Promise(resolve => setTimeout(resolve, 10))

        // B also locks using mirror table - has to wait
        const txB = (async () => {
          await clientB.query('BEGIN')
          await clientB.query('SELECT 1 FROM mirror WHERE row_id = $1 FOR UPDATE', [rowId])

          const result = await clientB.query(
            'INSERT INTO mutations (row_id, new_value) VALUES ($1, $2) RETURNING seq',
            [rowId, 222]
          )
          const seq = parseInt(result.rows[0].seq)

          await clientB.query('UPDATE mirror SET value = $1 WHERE row_id = $2', [222, rowId])
          await clientB.query('COMMIT')
          return seq
        })()

        await Promise.all([txA, txB])

        const mutations = await getMutationsInOrder(pool, rowId)
        const mirrorValue = await getMirrorValue(pool, rowId)
        const replayResult = mutations[mutations.length - 1].new_value

        expect(replayResult).toBe(mirrorValue)
        console.log('Mirror table locking works too:', replayResult, '===', mirrorValue)
      } catch (e) {
        await safeRollback(clientA)
        await safeRollback(clientB)
        throw e
      } finally {
        clientA.release()
        clientB.release()
      }
    })

    it('multiple rows can be mutated concurrently (no cross-row blocking)', async () => {
      const rowId1 = await insertTestRow(pool, `row-1-${Date.now()}`)
      const rowId2 = await insertTestRow(pool, `row-2-${Date.now()}`)

      const client1 = await pool.connect()
      const client2 = await pool.connect()

      const startTime = Date.now()

      try {
        // Helper function that does locking mutation
        async function mutateWithLocking(client: PoolClient, rowId: number, value: number, delay: number) {
          await client.query('BEGIN')
          await client.query('SELECT 1 FROM rows WHERE id = $1 FOR UPDATE', [rowId])
          await client.query('INSERT INTO mutations (row_id, new_value) VALUES ($1, $2)', [rowId, value])
          await client.query('UPDATE mirror SET value = $1 WHERE row_id = $2', [value, rowId])
          await new Promise(resolve => setTimeout(resolve, delay))
          await client.query('COMMIT')
        }

        // These should run in parallel since they lock different rows
        await Promise.all([
          mutateWithLocking(client1, rowId1, 100, 100), // 100ms delay
          mutateWithLocking(client2, rowId2, 200, 100), // 100ms delay
        ])

        const elapsed = Date.now() - startTime

        // If they blocked each other, it would take 200ms+
        // Since they run in parallel on different rows, should be ~100ms
        expect(elapsed).toBeLessThan(180)
        console.log(`Parallel mutations on different rows took ${elapsed}ms (should be ~100ms)`)
      } catch (e) {
        await safeRollback(client1)
        await safeRollback(client2)
        throw e
      } finally {
        client1.release()
        client2.release()
      }
    })
  })
})
