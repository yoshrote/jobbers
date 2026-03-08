import { useEffect, useState } from 'react'
import { getDLQ, getDLQHistory, removeManyFromDLQ, resubmitFromDLQ } from '../api/client'
import StatusBadge from '../components/StatusBadge'
import TaskNameSelect from '../components/TaskNameSelect'

export default function DeadLetterQueue() {
  const [filter, setFilter] = useState({ queue: '', task_name: '', limit: '100' })
  const [tasks,    setTasks]    = useState([])
  const [selected, setSelected] = useState(new Set())
  const [history,  setHistory]  = useState(null)  // { task_id, history }
  const [loading,  setLoading]  = useState(false)
  const [error,    setError]    = useState(null)
  const [msg,      setMsg]      = useState(null)

  function setF(field) {
    return (e) => setFilter((f) => ({ ...f, [field]: e.target.value }))
  }

  function load() {
    setLoading(true)
    setError(null)
    setMsg(null)
    const params = {
      queue:        filter.queue       || undefined,
      task_name:    filter.task_name   || undefined,
      limit:        Number(filter.limit),
    }
    getDLQ(params)
      .then((d) => { setTasks(d.tasks ?? []); setSelected(new Set()) })
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
    setSelected((prev) => prev.size === tasks.length ? new Set() : new Set(tasks.map((t) => t.id)))
  }

  async function handleResubmit(byIds) {
    const body = byIds
      ? { task_ids: [...selected] }
      : {
          queue:        filter.queue       || undefined,
          task_name:    filter.task_name   || undefined,
          limit:        Number(filter.limit),
        }
    try {
      const res = await resubmitFromDLQ({ reset_retry_count: true, ...body })
      setMsg(`Resubmitted ${res.resubmitted} task(s).`)
      load()
    } catch (e) {
      setError(e.message)
    }
  }

  async function handleDelete() {
    try {
      const res = await removeManyFromDLQ([...selected])
      setMsg(`Deleted ${res.removed} task(s).`)
      load()
    } catch (e) {
      setError(e.message)
    }
  }

  async function showHistory(taskId) {
    try {
      const res = await getDLQHistory(taskId)
      setHistory(res)
    } catch (e) {
      setError(e.message)
    }
  }

  return (
    <div>
      <h1>Dead Letter Queue</h1>

      <div className="card">
        <div className="filter-bar">
          <div className="form-row">
            <label>Queue</label>
            <input value={filter.queue} onChange={setF('queue')} placeholder="any" />
          </div>
          <div className="form-row">
            <label>Task name</label>
            <TaskNameSelect value={filter.task_name} onChange={(v) => setFilter((f) => ({ ...f, task_name: v }))} />
          </div>
          <div className="form-row">
            <label>Limit</label>
            <select value={filter.limit} onChange={setF('limit')} style={{ width: 80 }}>
              {[50, 100, 500, 1000].map((n) => <option key={n}>{n}</option>)}
            </select>
          </div>
          <button className="btn btn-primary" onClick={load}>Search</button>
          <button className="btn btn-secondary" onClick={() => handleResubmit(false)}>
            Resubmit all matching
          </button>
        </div>
      </div>

      {msg   && <p style={{ color: 'green' }}>{msg}</p>}
      {error && <p className="error-msg">{error}</p>}
      {loading && <p className="loading-msg">Loading…</p>}

      {selected.size > 0 && (
        <div style={{ marginBottom: '.75rem', display: 'flex', gap: '.5rem', alignItems: 'center' }}>
          <span>{selected.size} selected</span>
          <button className="btn btn-primary btn-sm" onClick={() => handleResubmit(true)}>
            Resubmit selected
          </button>
              <button className="btn btn-danger btn-sm" onClick={() => handleDelete()}>Delete selected</button>
          <button className="btn btn-secondary btn-sm" onClick={() => setSelected(new Set())}>Clear</button>
        </div>
      )}

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
              <th>Retries</th>
              <th>Last error</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {!loading && tasks.length === 0 && (
              <tr><td colSpan={7} className="empty-msg" style={{ padding: '1rem' }}>No dead-lettered tasks.</td></tr>
            )}
            {tasks.map((t) => (
              <tr key={t.id}>
                <td><input type="checkbox" checked={selected.has(t.id)} onChange={() => toggleSelect(t.id)} /></td>
                <td className="monospace" style={{ fontSize: 11 }}>{t.id}</td>
                <td>{t.name}</td>
                <td><StatusBadge status={t.status} /></td>
                <td>{t.retry_attempt}</td>
                <td style={{ color: '#dc3545', maxWidth: 300, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                  {t.last_error ?? '—'}
                </td>
                <td>
                  <button className="btn btn-secondary btn-sm" onClick={() => showHistory(t.id)}>
                    History
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* History modal */}
      {history && (
        <div style={{ position: 'fixed', inset: 0, background: '#0007', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 200 }}>
          <div className="card" style={{ maxWidth: 640, width: '90%', maxHeight: '80vh', overflow: 'auto' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <h2 style={{ margin: 0 }}>Failure history</h2>
              <button className="btn btn-secondary btn-sm" onClick={() => setHistory(null)}>Close</button>
            </div>
            <p className="monospace" style={{ fontSize: 12, color: '#666' }}>{history.task_id}</p>
            {history.history?.length ? (
              <table>
                <thead>
                  <tr><th>#</th><th>Timestamp</th><th>Error</th></tr>
                </thead>
                <tbody>
                  {history.history.map((h, i) => (
                    <tr key={i}>
                      <td>{h.retry_attempt ?? i + 1}</td>
                      <td>{h.timestamp ? new Date(h.timestamp).toLocaleString() : '—'}</td>
                      <td style={{ color: '#dc3545' }}>{h.error}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            ) : (
              <p className="empty-msg">No history entries.</p>
            )}
          </div>
        </div>
      )}
    </div>
  )
}
