import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { getAllQueues, getRegisteredTasks, getRoles } from '../api/client'

export default function Dashboard() {
  const [index, setIndex]   = useState(null)
  const [queues, setQueues] = useState(null)
  const [roles, setRoles]   = useState(null)
  const [error, setError]   = useState(null)

  useEffect(() => {
    Promise.all([getRegisteredTasks(), getAllQueues(), getRoles()])
      .then(([idx, q, r]) => {
        setIndex(idx)
        setQueues(q.queues)
        setRoles(r.roles)
      })
      .catch((e) => setError(e.message))
  }, [])

  if (error) return <p className="error-msg">{error}</p>
  if (!index) return <p className="loading-msg">Loading…</p>

  return (
    <div>
      <h1>Dashboard</h1>

      <div className="card-grid">
        <div className="stat-card">
          <div className="stat-value">{index.tasks?.length ?? 0}</div>
          <div className="stat-label">Registered task types</div>
        </div>
        <div className="stat-card">
          <div className="stat-value">{roles?.length ?? 0}</div>
          <div className="stat-label">Roles</div>
        </div>
        <div className="stat-card">
          <div className="stat-value">
            {queues ? Object.values(queues).flat().length : '—'}
          </div>
          <div className="stat-label">Total queues</div>
        </div>
      </div>

      <div className="card">
        <h2>Registered task types</h2>
        {index.tasks?.length ? (
          <ul>
            {index.tasks.map((t) => (
              <li key={t}>
                <Link to={`/tasks?task_name=${encodeURIComponent(t)}`}>{t}</Link>
              </li>
            ))}
          </ul>
        ) : (
          <p className="empty-msg">No task types registered.</p>
        )}
      </div>

      <div className="card">
        <h2>Quick links</h2>
        <ul>
          <li><Link to="/tasks">Browse tasks</Link></li>
          <li><Link to="/scheduled">Scheduled tasks</Link></li>
          <li><Link to="/dlq">Dead letter queue</Link></li>
          <li><Link to="/queues">Manage queues</Link></li>
          <li><Link to="/submit">Submit a task</Link></li>
        </ul>
      </div>
    </div>
  )
}
