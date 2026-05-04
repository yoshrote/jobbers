import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
// Import the named exports we want to exercise via the internal request() function.
// We test behaviour at the boundary: what URLs are fetched, what headers are sent,
// and how errors are surfaced.

// We need to intercept fetch before the module is loaded so we stub it globally.
const mockFetch = vi.fn()
globalThis.fetch = mockFetch

// Re-import after stub is in place.
const { getTaskList, submitTask, getDLQ, cancelTask } = await import('../../src/api/client.js')

function makeResponse(data, ok = true, status = 200) {
  return {
    ok,
    status,
    json: () => Promise.resolve(data),
  }
}

describe('api/client', () => {
  beforeEach(() => mockFetch.mockReset())

  describe('URL construction', () => {
    it('appends non-empty, non-null params as a query string', async () => {
      mockFetch.mockResolvedValue(makeResponse({ tasks: [] }))
      await getTaskList({ queue: 'default', limit: 20 })

      const [url] = mockFetch.mock.calls[0]
      expect(url).toContain('queue=default')
      expect(url).toContain('limit=20')
    })

    it('omits null and empty-string params from the query string', async () => {
      mockFetch.mockResolvedValue(makeResponse({ tasks: [] }))
      await getTaskList({ queue: 'default', task_name: '', task_version: null })

      const [url] = mockFetch.mock.calls[0]
      expect(url).not.toContain('task_name')
      expect(url).not.toContain('task_version')
    })

    it('sends no query string when all params are empty/null', async () => {
      mockFetch.mockResolvedValue(makeResponse({ tasks: [] }))
      await getTaskList({ queue: 'default' })

      const [url] = mockFetch.mock.calls[0]
      // Should not have a bare '?' with nothing after it
      expect(url).not.toMatch(/\?$/)
    })
  })

  describe('request headers and body', () => {
    it('sends Content-Type: application/json when there is a body', async () => {
      mockFetch.mockResolvedValue(makeResponse({ id: '123' }))
      await submitTask({ id: '01J', name: 'my_task', queue: 'default', version: 1, parameters: {} })

      const [, options] = mockFetch.mock.calls[0]
      expect(options.headers).toEqual({ 'Content-Type': 'application/json' })
      expect(options.method).toBe('POST')
    })

    it('sends no Content-Type header for GET requests', async () => {
      mockFetch.mockResolvedValue(makeResponse({ tasks: [] }))
      await getDLQ({})

      const [, options] = mockFetch.mock.calls[0]
      expect(options.headers).toBeUndefined()
      expect(options.method).toBe('GET')
    })

    it('serialises the body as JSON', async () => {
      mockFetch.mockResolvedValue(makeResponse({ id: 'x' }))
      const task = { id: '01J', name: 'my_task', queue: 'default', version: 1, parameters: { n: 42 } }
      await submitTask(task)

      const [, options] = mockFetch.mock.calls[0]
      expect(JSON.parse(options.body)).toEqual(task)
    })
  })

  describe('error handling', () => {
    it('throws with data.detail when the response is not ok', async () => {
      mockFetch.mockResolvedValue(makeResponse({ detail: 'Task not found' }, false, 404))
      await expect(cancelTask('bad-id')).rejects.toThrow('Task not found')
    })

    it('falls back to "HTTP <status>" when detail is absent', async () => {
      mockFetch.mockResolvedValue(makeResponse({}, false, 500))
      await expect(cancelTask('bad-id')).rejects.toThrow('HTTP 500')
    })
  })
})
