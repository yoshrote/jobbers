import { useEffect, useState } from 'react'
import { Link, useNavigate, useSearchParams } from 'react-router-dom'
import { cancelTasks, getTaskList } from '../api/client'
import StatusBadge from '../components/StatusBadge'

export default function TaskList() {
  const [searchParams, setSearchParams] = useSearchParams()
  const navigate = useNavigate()

  // Filter state – initialise from URL query params
  const [queue,       setQueue]       = useState(searchParams.get('queue')       ?? '')
  const [taskName,    setTaskName]    = useState(searchParams.get('task_name')   ?? '')
  const [taskVersion, setTaskVersion] = useState(searchParams.get('task_version') ?? '')
  const [startAfter,  setStartAfter]  = useState(searchParams.get('start')       ?? '')
  const [limit,       setLimit]       = useState(searchParams.get('limit')       ?? '20')
  const [orderBy,     setOrderBy]     = useState(searchParams.get('order_by')    ?? 'submitted_at')

  const [tasks,     setTasks]     = useState([])
  const [selected,  setSelected]  = useState(new Set())
  const [loading,   setLoading]   = useState(false)
  const [error,     setError]     = useState(null)

  function load() {
    if (!queue) return
    setLoading(true)
    setError(null)
    getTaskList({
      queue,
      start: startAfter || undefined,
      limit,
      order_by: orderBy,
      task_name: taskName || undefined,
      task_version: taskVersion || undefined,
    })
      .then((d) => setTasks(d.tasks ?? []))
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false))
  }

  useEffect(() => {
    load()
    setSearchParams({ queue, task_name: taskName, start: startAfter, limit, order_by: orderBy }, { replace: true })
  }, [queue, taskName, taskVersion, startAfter, limit, orderBy]) // eslint-disable-line react-hooks/exhaustive-deps

  function toggleSelect(id) {
    setSelected((prev) => {
      const next = new Set(prev)
      next.has(id) ? next.delete(id) : next.add(id)
      return next
    })
  }

  function toggleAll() {
    setSelected((prev) =>
      prev.size === tasks.length ? new Set() : new Set(tasks.map((t) => t.id))
    )
  }

  async function cancelSelected() {
    if (!selected.size) return
    if (!window.confirm(`Cancel ${selected.size} task(s)?`)) return
    try {
      await cancelTasks([...selected])
      load()
      setSelected(new Set())
    } catch (e) {
      alert(e.message)
    }
  }

  return (
    <div>
      <h1>Tasks</h1>

      {/* Filter bar */}
      <div className="card">
        <div className="filter-bar">
          <div className="form-row">
            <label>Queue *</label>
            <input value={queue} onChange={(e) => setQueue(e.target.value)} placeholder="default" />
          </div>
          <div className="form-row">
            <label>Task name</label>
            <input value={taskName} onChange={(e) => setTaskName(e.target.value)} placeholder="any" />
          </div>
          <div className="form-row">
            <label>Version</label>
            <input type="number" value={taskVersion} onChange={(e) => setTaskVersion(e.target.value)} style={{ width: 70 }} />
          </div>
          <div className="form-row">
            <label>Start after</label>
            <input
              value={startAfter}
              onChange={(e) => setStartAfter(e.target.value)}
              placeholder="ULID (optional)"
            />
          </div>
          <div className="form-row">
            <label>Order by</label>
            <select value={orderBy} onChange={(e) => setOrderBy(e.target.value)}>
              <option value="submitted_at">Submitted at</option>
              <option value="task_id">Task ID</option>
            </select>
          </div>
          <div className="form-row">
            <label>Limit</label>
            <select value={limit} onChange={(e) => setLimit(e.target.value)} style={{ width: 80 }}>
              {[10, 20, 50, 100].map((n) => <option key={n}>{n}</option>)}
            </select>
          </div>
          <button className="btn btn-primary" onClick={load}>Search</button>
        </div>
      </div>

      {/* Bulk actions */}
      {selected.size > 0 && (
        <div style={{ marginBottom: '.75rem', display: 'flex', gap: '.5rem', alignItems: 'center' }}>
          <span>{selected.size} selected</span>
          <button className="btn btn-danger btn-sm" onClick={cancelSelected}>Cancel selected</button>
          <button className="btn btn-secondary btn-sm" onClick={() => setSelected(new Set())}>Clear</button>
        </div>
      )}

      {error && <p className="error-msg">{error}</p>}
      {loading && <p className="loading-msg">Loading…</p>}

      {!loading && (
        <div className="card" style={{ padding: 0 }}>
          <table>
            <thead>
              <tr>
                <th style={{ width: 32 }}>
                  <input type="checkbox" checked={selected.size === tasks.length && tasks.length > 0} onChange={toggleAll} />
                </th>
                <th>ID</th>
                <th>Name</th>
                <th>Status</th>
                <th>Retry</th>
                <th>Submitted at</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              {tasks.length === 0 && (
                <tr><td colSpan={7} className="empty-msg" style={{ padding: '1rem' }}>No tasks found.</td></tr>
              )}
              {tasks.map((t) => (
                <tr key={t.id}>
                  <td>
                    <input type="checkbox" checked={selected.has(t.id)} onChange={() => toggleSelect(t.id)} />
                  </td>
                  <td>
                    <Link to={`/tasks/${t.id}`} className="task-id-link">{t.id}</Link>
                  </td>
                  <td>{t.name}</td>
                  <td><StatusBadge status={t.status} /></td>
                  <td>{t.retry_attempt}</td>
                  <td>{t.submitted_at ? new Date(t.submitted_at).toLocaleString() : '—'}</td>
                  <td>
                    <button className="btn btn-secondary btn-sm" onClick={() => navigate(`/tasks/${t.id}`)}>
                      View
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
