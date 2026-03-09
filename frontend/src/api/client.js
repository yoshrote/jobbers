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

const get  = (path, params) => request('GET',    path, null, params)
const post = (path, body)   => request('POST',   path, body)
const put  = (path, body)   => request('PUT',    path, body)
const del  = (path)         => request('DELETE', path)

// ── Registered tasks ───────────────────────────────────────────────────────

/** GET /  →  { message, tasks: [string, number][] } */
export const getRegisteredTasks = () => get('/')

// ── Tasks ──────────────────────────────────────────────────────────────────

/**
 * GET /task-list
 * @param {{
 *   queue: string,
 *   limit?: number,
 *   start?: string,
 *   order_by?: 'submitted_at' | 'task_id',
 *   task_name?: string,
 *   task_version?: number
 * }} params
 */
export const getTaskList = (params) => get('/task-list', params)

/** GET /task-status/{task_id}  →  task summary object */
export const getTaskStatus = (taskId) => get(`/task-status/${taskId}`)

/**
 * POST /submit-task
 * @param {{
 *   id: string,
 *   name: string,
 *   queue?: string,
 *   version?: number,
 *   parameters?: object,
 *   results?: object,
 *   errors?: string[],
 *   retry_attempt?: number,
 *   status?: string,
 *   submitted_at?: string,
 *   retried_at?: string,
 *   started_at?: string,
 *   heartbeat_at?: string,
 *   completed_at?: string,
 *   task_config?: object
 * }} task
 */
export const submitTask = (task) => post('/submit-task', task)

/** POST /task/{task_id}/cancel  →  { message, task } */
export const cancelTask = (taskId) => post(`/task/${taskId}/cancel`)

/**
 * POST /tasks/cancel
 * @param {string[]} taskIds
 */
export const cancelTasks = (taskIds) => post('/tasks/cancel', { task_ids: taskIds })

/**
 * GET /active-tasks
 * @param {{ queue?: string }} [params]
 */
export const getActiveTasks = (params) => get('/active-tasks', params)

// ── Scheduled tasks ────────────────────────────────────────────────────────

/**
 * GET /scheduled-tasks
 * @param {{
 *   queue: string,
 *   limit?: number,
 *   start?: string,
 *   order_by?: 'submitted_at' | 'task_id',
 *   task_name?: string,
 *   task_version?: number
 * }} params
 */
export const getScheduledTasks = (params) => get('/scheduled-tasks', params)

// ── Dead letter queue ──────────────────────────────────────────────────────

/**
 * GET /dead-letter-queue
 * @param {{ queue?: string, task_name?: string, task_version?: number, limit?: number }} params
 */
export const getDLQ = (params) => get('/dead-letter-queue', params)

/** GET /dead-letter-queue/{task_id}/history  →  { task_id, history: [] } */
export const getDLQHistory = (taskId) => get(`/dead-letter-queue/${taskId}/history`)

/**
 * POST /dead-letter-queue/resubmit
 * @param {{
 *   task_ids?: string[],
 *   queue?: string,
 *   task_name?: string,
 *   task_version?: number,
 *   reset_retry_count?: boolean,
 *   limit?: number
 * }} body
 */
export const resubmitFromDLQ = (body) => post('/dead-letter-queue/resubmit', body)

/**
 * DELETE /dead-letter-queue
 * @param {string[]} taskIds
 */
export const removeManyFromDLQ = (taskIds) => request('DELETE', '/dead-letter-queue', { task_ids: taskIds })

// ── Queues ─────────────────────────────────────────────────────────────────

/** GET /queues  →  { queues: string[] } */
export const getAllQueues = () => get('/queues')

/**
 * POST /queues  →  201 { message, queue }
 * @param {{ name: string, max_concurrent?: number|null, rate_numerator?: number|null, rate_denominator?: number|null, rate_period?: 'second'|'minute'|'hour'|'day'|null }} config
 */
export const createQueue = (config) => post('/queues', config)

/** GET /queues/{queue_name}/config  →  { queue: QueueConfig } */
export const getQueueConfig = (name) => get(`/queues/${name}/config`)

/**
 * PUT /queues/{queue_name}  →  { message, queue }
 * @param {string} name
 * @param {{ name: string, max_concurrent?: number|null, rate_numerator?: number|null, rate_denominator?: number|null, rate_period?: 'second'|'minute'|'hour'|'day'|null }} config
 */
export const updateQueue = (name, config) => put(`/queues/${name}`, config)

/** DELETE /queues/{queue_name}  →  { message } */
export const deleteQueue = (name) => del(`/queues/${name}`)

// ── Roles ──────────────────────────────────────────────────────────────────

/** GET /roles  →  { roles: string[] } */
export const getRoles = () => get('/roles')

/** GET /roles/{role_name}  →  { role: string, queues: string[] } */
export const getRole = (name) => get(`/roles/${name}`)

/**
 * POST /roles  →  201 { message, role, queues }
 * @param {string} name
 * @param {string[]} queues
 */
export const createRole = (name, queues) => post('/roles', { name, queues })

/**
 * PUT /roles/{role_name}  →  { message, role, queues }
 * @param {string} name
 * @param {string[]} queues
 */
export const updateRole = (name, queues) => put(`/roles/${name}`, queues)

/** DELETE /roles/{role_name}  →  { message } */
export const deleteRole = (name) => del(`/roles/${name}`)
