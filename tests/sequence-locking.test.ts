import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest'
import { Pool } from 'pg'
import {
  createPool,
  setupSchema,
  insertTestRow,
  getMutationsInOrder,
  getMirrorValue,
  mutateWithoutLocking,
  mutateWithLocking,
} from '../src/db'

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

describe('PostgreSQL Sequence Locking', () => {
  let pool: Pool

  beforeAll(async () => {
    pool = createPool()
    await setupSchema(pool)
  })

  afterAll(async () => {
    await pool.end()
  })

  beforeEach(async () => {
    // Reset sequence and clear tables for each test
    await pool.query('TRUNCATE mutations, mirror, rows RESTART IDENTITY CASCADE')
    await pool.query('ALTER SEQUENCE mutations_seq RESTART WITH 1')
  })

  describe('WITHOUT locking (demonstrates the bug)', () => {
    it('should show that sequence order can differ from commit order', async () => {
      const rowId = await insertTestRow(pool, 'test-row')

      // We'll run this multiple times to catch the race condition
      let foundMismatch = false
      const attempts = 20

      for (let attempt = 0; attempt < attempts && !foundMismatch; attempt++) {
        // Reset for each attempt
        await pool.query('TRUNCATE mutations RESTART IDENTITY CASCADE')
        await pool.query('UPDATE mirror SET value = 0 WHERE row_id = $1', [rowId])
        await pool.query('ALTER SEQUENCE mutations_seq RESTART WITH 1')

        const clientA = await pool.connect()
        const clientB = await pool.connect()

        try {
          // Coordinate the race condition:
          // 1. A starts transaction and gets sequence number
          // 2. B starts transaction and gets sequence number
          // 3. B commits first
          // 4. A commits second

          // Start A's transaction and get sequence, but DON'T commit yet
          await clientA.query('BEGIN')
          const resultA = await clientA.query(
            'INSERT INTO mutations (row_id, new_value) VALUES ($1, $2) RETURNING seq',
            [rowId, 100]
          )
          const seqA = parseInt(resultA.rows[0].seq)
          await clientA.query('UPDATE mirror SET value = $1 WHERE row_id = $2', [100, rowId])

          // Start B's transaction, get sequence, and commit immediately
          await clientB.query('BEGIN')
          const resultB = await clientB.query(
            'INSERT INTO mutations (row_id, new_value) VALUES ($1, $2) RETURNING seq',
            [rowId, 200]
          )
          const seqB = parseInt(resultB.rows[0].seq)
          await clientB.query('UPDATE mirror SET value = $1 WHERE row_id = $2', [200, rowId])
          await clientB.query('COMMIT')

          // Now A commits (after B)
          await clientA.query('COMMIT')

          // Check the results
          const mutations = await getMutationsInOrder(pool, rowId)
          const mirrorValue = await getMirrorValue(pool, rowId)

          // A got sequence 1, B got sequence 2
          expect(seqA).toBe(1)
          expect(seqB).toBe(2)

          // Mirror has A's value (100) because A committed LAST
          expect(mirrorValue).toBe(100)

          // But mutations show seq 1 (value=100) before seq 2 (value=200)
          // If we replayed these in order, we'd end up with 200, not 100!
          expect(mutations[0].new_value).toBe(100) // seq 1
          expect(mutations[1].new_value).toBe(200) // seq 2

          // The final value in mirror (100) != what we'd get from replaying (200)
          // This is the bug!
          const replayedValue = mutations[mutations.length - 1].new_value
          if (mirrorValue !== replayedValue) {
            foundMismatch = true
            console.log(`Found mismatch on attempt ${attempt + 1}:`)
            console.log(`  Mirror value: ${mirrorValue} (A committed last)`)
            console.log(`  Replayed value: ${replayedValue} (B has higher sequence)`)
            console.log(`  Mutations: ${JSON.stringify(mutations)}`)
          }
        } finally {
          clientA.release()
          clientB.release()
        }
      }

      // We should have found the mismatch - this proves the bug exists
      expect(foundMismatch).toBe(true)
    })

    it('demonstrates the race more clearly with controlled timing', async () => {
      const rowId = await insertTestRow(pool, 'timing-test')

      const clientA = await pool.connect()
      const clientB = await pool.connect()

      const events: string[] = []

      try {
        // Transaction A: starts first, gets seq first, but commits LAST
        const txA = (async () => {
          await clientA.query('BEGIN')
          events.push('A: BEGIN')

          const result = await clientA.query(
            'INSERT INTO mutations (row_id, new_value) VALUES ($1, $2) RETURNING seq',
            [rowId, 111]
          )
          const seq = parseInt(result.rows[0].seq)
          events.push(`A: got seq ${seq}`)

          await clientA.query('UPDATE mirror SET value = $1 WHERE row_id = $2', [111, rowId])
          events.push('A: updated mirror to 111')

          // Wait for B to commit first
          await new Promise(resolve => setTimeout(resolve, 100))

          await clientA.query('COMMIT')
          events.push('A: COMMIT')

          return seq
        })()

        // Transaction B: starts after A gets seq, but commits FIRST
        await new Promise(resolve => setTimeout(resolve, 10)) // Let A start

        const txB = (async () => {
          await clientB.query('BEGIN')
          events.push('B: BEGIN')

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

        // Verify the sequence assignment order
        expect(seqA).toBeLessThan(seqB) // A got sequence first

        // But B committed first, so mirror has A's value (committed last wins)
        const mirrorValue = await getMirrorValue(pool, rowId)
        expect(mirrorValue).toBe(111) // A's value, because A committed last

        // The mutations in sequence order
        const mutations = await getMutationsInOrder(pool, rowId)
        expect(mutations.map(m => m.new_value)).toEqual([111, 222])

        // Replaying would give us 222 (last in sequence order)
        // But the actual final state is 111 (last to commit)
        // THIS IS THE BUG
        const replayResult = mutations[mutations.length - 1].new_value
        expect(replayResult).toBe(222)
        expect(mirrorValue).toBe(111)
        expect(replayResult).not.toBe(mirrorValue) // They don't match!

        console.log('BUG DEMONSTRATED:')
        console.log(`  Sequence order: ${mutations.map(m => `seq${m.seq}=${m.new_value}`).join(' -> ')}`)
        console.log(`  Replay would produce: ${replayResult}`)
        console.log(`  Actual mirror value: ${mirrorValue}`)
      } finally {
        clientA.release()
        clientB.release()
      }
    })
  })

  describe('WITH locking (the fix)', () => {
    it('should guarantee sequence order matches commit order', async () => {
      const rowId = await insertTestRow(pool, 'locked-row')

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

        console.log('FIX VERIFIED:')
        console.log(`  Sequence order: ${mutations.map(m => `seq${m.seq}=${m.new_value}`).join(' -> ')}`)
        console.log(`  Replay would produce: ${replayResult}`)
        console.log(`  Actual mirror value: ${mirrorValue}`)
        console.log('  ✓ They match!')
      } finally {
        clientA.release()
        clientB.release()
      }
    })

    it('should work correctly under concurrent load', async () => {
      const rowId = await insertTestRow(pool, 'concurrent-test')

      // Run many concurrent mutations with locking
      const numMutations = 20
      const promises: Promise<{ seq: number; value: number }>[] = []

      for (let i = 1; i <= numMutations; i++) {
        const value = i * 10
        promises.push(
          (async () => {
            const client = await pool.connect()
            try {
              const seq = await mutateWithLocking(client, rowId, value)
              return { seq, value }
            } finally {
              client.release()
            }
          })()
        )
      }

      const results = await Promise.all(promises)

      // Get final state
      const mutations = await getMutationsInOrder(pool, rowId)
      const mirrorValue = await getMirrorValue(pool, rowId)

      // Find which mutation committed last (highest sequence)
      const lastMutation = mutations[mutations.length - 1]

      console.log(`Total mutations: ${mutations.length}`)
      console.log(`Last sequence: ${lastMutation.seq}, value: ${lastMutation.new_value}`)
      console.log(`Mirror value: ${mirrorValue}`)

      // With locking, the last sequence number's value should match mirror
      expect(mirrorValue).toBe(lastMutation.new_value)

      // Verify all sequences are unique and in order
      const seqs = mutations.map(m => m.seq)
      const sortedSeqs = [...seqs].sort((a, b) => a - b)
      expect(seqs).toEqual(sortedSeqs) // Already in order

      // Verify no gaps (sequences should be consecutive)
      for (let i = 1; i < seqs.length; i++) {
        expect(seqs[i]).toBe(seqs[i - 1] + 1)
      }
    })

    it('should allow replay to reconstruct correct final state', async () => {
      const rowId = await insertTestRow(pool, 'replay-test')

      // Perform a series of mutations with locking
      const values = [10, 20, 15, 30, 25]

      for (const value of values) {
        const client = await pool.connect()
        try {
          await mutateWithLocking(client, rowId, value)
        } finally {
          client.release()
        }
      }

      // Get all mutations in sequence order
      const mutations = await getMutationsInOrder(pool, rowId)
      const mirrorValue = await getMirrorValue(pool, rowId)

      // "Replay" the mutations
      let replayedValue = 0
      for (const mutation of mutations) {
        replayedValue = mutation.new_value
      }

      console.log('Mutations in order:', mutations.map(m => m.new_value))
      console.log('Replayed final value:', replayedValue)
      console.log('Actual mirror value:', mirrorValue)

      // With locking, replay produces the same result as the actual state
      expect(replayedValue).toBe(mirrorValue)
    })
  })

  describe('Edge cases', () => {
    it('locking on mirror table instead of rows table also works', async () => {
      const rowId = await insertTestRow(pool, 'mirror-lock-test')

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
      } finally {
        clientA.release()
        clientB.release()
      }
    })

    it('multiple rows can be mutated concurrently (no cross-row blocking)', async () => {
      const rowId1 = await insertTestRow(pool, 'row-1')
      const rowId2 = await insertTestRow(pool, 'row-2')

      const client1 = await pool.connect()
      const client2 = await pool.connect()

      const startTime = Date.now()

      try {
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
      } finally {
        client1.release()
        client2.release()
      }
    })
  })
})
