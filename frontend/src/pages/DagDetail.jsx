import mermaid from 'mermaid'
import { useEffect, useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import { getDag, getTaskStatus } from '../api/client'
import StatusBadge from '../components/StatusBadge'

mermaid.initialize({ startOnLoad: false, theme: 'default' })

function MermaidDiagram({ diagram }) {
  const [svg, setSvg] = useState('')
  const [err, setErr] = useState(null)

  useEffect(() => {
    if (!diagram) return
    let cancelled = false
    const id = `dag-detail-${Date.now()}`
    mermaid.render(id, diagram)
      .then(({ svg }) => { if (!cancelled) { setSvg(svg); setErr(null) } })
      .catch((e) => { if (!cancelled) { setSvg(''); setErr(e.message ?? 'Render error') } })
    return () => { cancelled = true }
  }, [diagram])

  if (err) return <p className="error-msg">Could not render diagram: {err}</p>
  if (!svg) return <p className="loading-msg">Rendering diagram…</p>
  return <div dangerouslySetInnerHTML={{ __html: svg }} style={{ overflowX: 'auto' }} />
}

function formatTs(ts) {
  if (!ts) return '—'
  return new Date(ts).toLocaleString()
}

export default function DagDetail() {
  const { dagRunId } = useParams()
  const [run, setRun]         = useState(null)
  const [tasks, setTasks]     = useState([])
  const [diagram, setDiagram] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError]     = useState(null)

  async function load() {
    setLoading(true)
    setError(null)
    try {
      const dagRun = await getDag(dagRunId)
      setRun(dagRun)

      const taskResults = await Promise.all(
        (dagRun.task_ids ?? []).map((id) => getTaskStatus(id).catch(() => ({ id, error: true })))
      )
      setTasks(taskResults)

      const root = taskResults.find((t) => t.dag_diagram)
      if (root) setDiagram(root.dag_diagram)
    } catch (e) {
      setError(e.message)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { load() }, [dagRunId]) // eslint-disable-line react-hooks/exhaustive-deps

  if (loading) return <p className="loading-msg">Loading…</p>
  if (error)   return <p className="error-msg">{error}</p>
  if (!run)    return <p className="empty-msg">DAG run not found.</p>

  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'center', gap: '1rem', marginBottom: '1rem' }}>
        <h1 style={{ margin: 0 }}>DAG Run</h1>
        <button className="btn btn-secondary" onClick={load}>Refresh</button>
      </div>

      <div className="card">
        <table style={{ width: 'auto' }}>
          <tbody>
            <tr><th>DAG Run ID</th><td className="monospace" style={{ fontSize: '0.85rem' }}>{run.dag_run_id}</td></tr>
            <tr><th>Submitted at</th><td>{formatTs(run.submitted_at)}</td></tr>
            <tr><th>Tasks</th><td>{run.task_ids?.length ?? 0}</td></tr>
          </tbody>
        </table>
      </div>

      {diagram && (
        <div className="card">
          <h2 style={{ marginTop: 0 }}>DAG Structure</h2>
          <MermaidDiagram diagram={diagram} />
        </div>
      )}

      <div className="card">
        <h2 style={{ marginTop: 0 }}>Tasks</h2>
        {tasks.length === 0 ? (
          <p className="empty-msg">No tasks found.</p>
        ) : (
          <table style={{ width: '100%' }}>
            <thead>
              <tr>
                <th>Task ID</th>
                <th>Name</th>
                <th>Status</th>
                <th>Retry</th>
                <th>Submitted at</th>
                <th>Last error</th>
              </tr>
            </thead>
            <tbody>
              {tasks.map((task) => (
                <tr key={task.id}>
                  <td>
                    <Link to={`/tasks/${task.id}`} className="monospace" style={{ fontSize: '0.82rem' }}>
                      {task.id}
                    </Link>
                  </td>
                  <td>{task.name ?? '—'}</td>
                  <td>{task.status ? <StatusBadge status={task.status} /> : '—'}</td>
                  <td style={{ textAlign: 'center' }}>{task.retry_attempt ?? '—'}</td>
                  <td style={{ whiteSpace: 'nowrap' }}>{formatTs(task.submitted_at)}</td>
                  <td style={{ color: '#dc3545', maxWidth: 300, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                    {task.last_error ?? ''}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  )
}
