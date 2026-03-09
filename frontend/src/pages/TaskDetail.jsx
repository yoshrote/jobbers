import { useEffect, useState } from 'react'
import { useParams } from 'react-router-dom'
import { cancelTask, getTaskStatus } from '../api/client'
import StatusBadge from '../components/StatusBadge'

const CANCELLABLE = new Set(['submitted', 'started', 'scheduled'])

export default function TaskDetail() {
  const { taskId } = useParams()
  const [task,    setTask]    = useState(null)
  const [loading, setLoading] = useState(true)
  const [error,   setError]   = useState(null)
  const [msg,     setMsg]     = useState(null)

  function load() {
    setLoading(true)
    setError(null)
    getTaskStatus(taskId)
      .then(setTask)
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false))
  }

  useEffect(() => { load() }, [taskId]) // eslint-disable-line react-hooks/exhaustive-deps

  async function handleCancel() {
    if (!window.confirm('Request cancellation for this task?')) return
    try {
      const res = await cancelTask(taskId)
      setMsg(res.message)
      load()
    } catch (e) {
      setError(e.message)
    }
  }

  if (loading) return <p className="loading-msg">Loading…</p>
  if (error)   return <p className="error-msg">{error}</p>
  if (!task)   return <p className="empty-msg">Task not found.</p>

  const canCancel = CANCELLABLE.has(task.status?.toLowerCase())

  return (
    <div>
      <h1>Task detail</h1>
      <p className="monospace" style={{ color: '#666', marginTop: 0 }}>{task.id}</p>

      {msg && <p style={{ color: 'green' }}>{msg}</p>}

      <div className="card">
        <table style={{ width: 'auto' }}>
          <tbody>
            <tr><th>Name</th><td>{task.name}</td></tr>
            <tr><th>Status</th><td><StatusBadge status={task.status} /></td></tr>
            <tr><th>Retry attempt</th><td>{task.retry_attempt}</td></tr>
            <tr>
              <th>Submitted at</th>
              <td>{task.submitted_at ? new Date(task.submitted_at).toLocaleString() : '—'}</td>
            </tr>
            {task.last_error && (
              <tr><th>Last error</th><td style={{ color: '#dc3545' }}>{task.last_error}</td></tr>
            )}
          </tbody>
        </table>

        <div style={{ marginTop: '1rem', display: 'flex', gap: '.5rem' }}>
          <button className="btn btn-secondary" onClick={load}>Refresh</button>
          {canCancel && (
            <button className="btn btn-danger" onClick={handleCancel}>Request cancellation</button>
          )}
        </div>
      </div>

      <div className="card">
        <h2>Parameters</h2>
        <pre>{JSON.stringify(task.parameters ?? {}, null, 2)}</pre>
      </div>
    </div>
  )
}
