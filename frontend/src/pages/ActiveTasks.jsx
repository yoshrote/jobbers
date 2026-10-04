import { useEffect, useState } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import { cancelTasks, getActiveTasks } from '../api/client'
import StatusBadge from '../components/StatusBadge'

export default function ActiveTasks() {
  const navigate = useNavigate()

  const [queue,    setQueue]    = useState('')
  const [tasks,    setTasks]    = useState([])
  const [selected, setSelected] = useState(new Set())
  const [loading,  setLoading]  = useState(false)
  const [error,    setError]    = useState(null)

  function load() {
    setLoading(true)
    setError(null)
    getActiveTasks(queue ? { queue } : undefined)
      .then((d) => setTasks(d.tasks ?? []))
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false))
  }

  useEffect(() => { load() }, []) // eslint-disable-line react-hooks/exhaustive-deps

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
      <h1>Active Tasks</h1>

      {/* Filter bar */}
      <div className="card">
        <div className="filter-bar">
          <div className="form-row">
            <label>Queue</label>
            <input value={queue} onChange={(e) => setQueue(e.target.value)} placeholder="all queues" />
          </div>
          <button className="btn btn-primary" onClick={load}>Refresh</button>
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
                <th>Queue</th>
                <th>Status</th>
                <th>Retry</th>
                <th>Submitted at</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              {tasks.length === 0 && (
                <tr><td colSpan={8} className="empty-msg" style={{ padding: '1rem' }}>No active tasks.</td></tr>
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
                  <td>{t.queue ?? '—'}</td>
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
