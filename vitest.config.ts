import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    testTimeout: 60000,
    hookTimeout: 60000,
    fileParallelism: false, // Run test files sequentially
    pool: 'forks', // Use separate processes to avoid shared state
  },
})
