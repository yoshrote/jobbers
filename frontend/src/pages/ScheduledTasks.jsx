import { useState } from 'react'
import { Link } from 'react-router-dom'
import { getScheduledTasks } from '../api/client'
import StatusBadge from '../components/StatusBadge'

export default function ScheduledTasks() {
  const [filter, setFilter] = useState({ queue: '', task_name: '', task_version: '', start: '', limit: '20' })
  const [tasks,   setTasks]   = useState([])
  const [loading, setLoading] = useState(false)
  const [error,   setError]   = useState(null)
  const [searched, setSearched] = useState(false)

  function setF(field) {
    return (e) => setFilter((f) => ({ ...f, [field]: e.target.value }))
  }

  function load() {
    if (!filter.queue) { alert('Queue is required.'); return }
    setLoading(true)
    setError(null)
    const params = {
      queue:        filter.queue,
      start:        filter.start || undefined,
      task_name:    filter.task_name   || undefined,
      task_version: filter.task_version ? Number(filter.task_version) : undefined,
      limit:        Number(filter.limit),
    }
    getScheduledTasks(params)
      .then((d) => { setTasks(d.tasks ?? []); setSearched(true) })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false))
  }

  return (
    <div>
      <h1>Scheduled tasks</h1>

      <div className="card">
        <div className="filter-bar">
          <div className="form-row">
            <label>Queue *</label>
            <input value={filter.queue} onChange={setF('queue')} placeholder="default" />
          </div>
          <div className="form-row">
            <label>Task name</label>
            <input value={filter.task_name} onChange={setF('task_name')} placeholder="any" />
          </div>
          <div className="form-row">
            <label>Version</label>
            <input type="number" value={filter.task_version} onChange={setF('task_version')} style={{ width: 70 }} />
          </div>
          <div className="form-row">
            <label>Start after</label>
            <input value={filter.start} onChange={setF('start')} placeholder="ULID (optional)" />
          </div>
          <div className="form-row">
            <label>Limit</label>
            <select value={filter.limit} onChange={setF('limit')} style={{ width: 80 }}>
              {[10, 20, 50, 100].map((n) => <option key={n}>{n}</option>)}
            </select>
          </div>
          <button className="btn btn-primary" onClick={load}>Search</button>
        </div>
      </div>

      {error   && <p className="error-msg">{error}</p>}
      {loading && <p className="loading-msg">Loading…</p>}

      {!loading && searched && (
        <div className="card" style={{ padding: 0 }}>
          <table>
            <thead>
              <tr>
                <th>ID</th>
                <th>Name</th>
                <th>Status</th>
                <th>Retry</th>
                <th>Submitted at</th>
              </tr>
            </thead>
            <tbody>
              {tasks.length === 0 && (
                <tr><td colSpan={5} className="empty-msg" style={{ padding: '1rem' }}>No scheduled tasks.</td></tr>
              )}
              {tasks.map((t) => (
                <tr key={t.id}>
                  <td>
                    <Link to={`/tasks/${t.id}`} className="task-id-link">{t.id}</Link>
                  </td>
                  <td>{t.name}</td>
                  <td><StatusBadge status={t.status} /></td>
                  <td>{t.retry_attempt}</td>
                  <td>{t.submitted_at ? new Date(t.submitted_at).toLocaleString() : '—'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
