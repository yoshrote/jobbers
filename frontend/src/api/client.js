/**
 * Jobbers API client.
 *
 * All requests go through /api, which Vite proxies to the backend in dev.
 * Set VITE_API_BASE in .env to override (e.g. for production builds).
 */

const BASE = import.meta.env.VITE_API_BASE ?? '/api'

async function request(method, path, body, params) {
  let url = `${BASE}${path}`
  if (params) {
    const qs = new URLSearchParams(
      Object.fromEntries(Object.entries(params).filter(([, v]) => v != null && v !== ''))
    )
    if ([...qs].length) url += `?${qs}`
  }
  const res = await fetch(url, {
    method,
    headers: body ? { 'Content-Type': 'application/json' } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  })
  const data = await res.json()
  if (!res.ok) throw new Error(data.detail ?? `HTTP ${res.status}`)
  return data
}

const get  = (path, params) => request('GET',  path, null, params)
const post = (path, body)   => request('POST', path, body)

// ── Index ──────────────────────────────────────────────────────────────────

/** GET /  →  { message, tasks: string[] } */
export const getIndex = () => get('/')

// ── Tasks ──────────────────────────────────────────────────────────────────

/**
 * GET /task-list
 * @param {{ queue: string, limit?: number, start?: string, order_by?: string, task_name?: string, task_version?: number }} params
 */
export const getTaskList = (params) => get('/task-list', params)

/** GET /task-status/:taskId  →  task summary object */
export const getTaskStatus = (taskId) => get(`/task-status/${taskId}`)

/**
 * POST /submit-task
 * @param {{ id: string, name: string, queue?: string, version?: number, parameters?: object }} task
 */
export const submitTask = (task) => post('/submit-task', task)

/** POST /task/:taskId/cancel  →  { message, task } */
export const cancelTask = (taskId) => post(`/task/${taskId}/cancel`)

/**
 * POST /tasks/cancel
 * @param {string[]} taskIds
 */
export const cancelTasks = (taskIds) => post('/tasks/cancel', { task_ids: taskIds })

// ── Scheduled tasks ────────────────────────────────────────────────────────

/**
 * GET /scheduled-tasks
 * @param {{ queue: string, limit?: number, start?: string, task_name?: string, task_version?: number }} params
 */
export const getScheduledTasks = (params) => get('/scheduled-tasks', params)

// ── Dead letter queue ──────────────────────────────────────────────────────

/**
 * GET /dead-letter-queue
 * @param {{ queue?: string, task_name?: string, task_version?: number, limit?: number }} params
 */
export const getDLQ = (params) => get('/dead-letter-queue', params)

/** GET /dead-letter-queue/:taskId/history  →  { task_id, history: [] } */
export const getDLQHistory = (taskId) => get(`/dead-letter-queue/${taskId}/history`)

/**
 * POST /dead-letter-queue/resubmit
 * @param {{ task_ids?: string[], queue?: string, task_name?: string, task_version?: number, reset_retry_count?: boolean, limit?: number }} body
 */
export const resubmitFromDLQ = (body) => post('/dead-letter-queue/resubmit', body)

// ── Queues ─────────────────────────────────────────────────────────────────

/** GET /queues  →  { queues: object } */
export const getAllQueues = () => get('/queues')

/** GET /queues/:role  →  { queues: string[] } */
export const getQueuesForRole = (role) => get(`/queues/${role}`)

/**
 * POST /queues/:role
 * @param {string[]} queues
 */
export const setQueuesForRole = (role, queues) => post(`/queues/${role}`, queues)

// ── Roles ──────────────────────────────────────────────────────────────────

/** GET /roles  →  { roles: string[] } */
export const getRoles = () => get('/roles')
