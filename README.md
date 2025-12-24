# PostgreSQL Mutation Sequence Locking Test

This project demonstrates a common PostgreSQL concurrency bug when using sequences to order mutations, and how to fix it using row-level locking.

## The Problem

Sequences increment at `nextval()` time, **not** at commit time. This means two concurrent transactions can get sequence numbers in one order but commit in a different order:

```
Transaction A                    Transaction B
─────────────────────────────────────────────────
BEGIN
INSERT (gets seq 100)
                                 BEGIN
                                 INSERT (gets seq 101)
                                 UPDATE mirror (value = 200)
                                 COMMIT                        ← B commits first
UPDATE mirror (value = 100)
COMMIT                                                         ← A commits last
```

**Result:**
- Mirror ends up with `value = 100` (A committed last)
- Mutations table shows: `seq 100 → value 100`, then `seq 101 → value 200`
- If you replay mutations in sequence order, you'd end up with `200`, not `100`!

## The Solution

Lock the row **before** getting the sequence number:

```sql
BEGIN;
SELECT 1 FROM rows WHERE id = $1 FOR UPDATE;  -- serialize here
INSERT INTO mutations (row_id, new_value) VALUES (...);  -- seq assigned after lock
UPDATE mirror SET value = $1 WHERE row_id = $2;
COMMIT;
```

Now Transaction B can't get its sequence number until A releases the lock at commit time. The sequence order is guaranteed to match the commit order.

## Running the Tests

1. Start PostgreSQL:
   ```bash
   docker-compose up -d
   ```

2. Install dependencies:
   ```bash
   npm install
   ```

3. Run tests:
   ```bash
   npm test
   ```

## Test Structure

- `tests/sequence-locking.test.ts` - Main test file with two describe blocks:
  - **WITHOUT locking** - Demonstrates the bug where sequence order differs from commit order
  - **WITH locking** - Proves the fix works by showing sequence order matches commit order

## Key Takeaways

1. PostgreSQL sequences are **not** tied to transactions - they increment immediately
2. For mutation ordering to work correctly, you must serialize access at the row level
3. `SELECT ... FOR UPDATE` is the standard way to achieve this serialization
4. You can lock either the main entity table (`rows`) or the mirror table - both work
