import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { listDags } from '../api/client'

const PAGE_SIZE = 25

export default function DagList() {
  const [dags, setDags]       = useState([])
  const [total, setTotal]     = useState(0)
  const [offset, setOffset]   = useState(0)
  const [loading, setLoading] = useState(true)
  const [error, setError]     = useState(null)

  function load(off = offset) {
    setLoading(true)
    setError(null)
    listDags({ limit: PAGE_SIZE, offset: off })
      .then((d) => { setDags(d.dags ?? []); setTotal(d.total ?? 0) })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false))
  }

  useEffect(() => { load(offset) }, [offset]) // eslint-disable-line react-hooks/exhaustive-deps

  const totalPages = Math.ceil(total / PAGE_SIZE)
  const page = Math.floor(offset / PAGE_SIZE) + 1

  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'center', gap: '1rem', marginBottom: '1rem' }}>
        <h1 style={{ margin: 0 }}>DAG Runs</h1>
        {!loading && <span style={{ color: '#666' }}>{total} total</span>}
        <button className="btn btn-secondary" onClick={() => load(offset)} style={{ marginLeft: 'auto' }}>
          Refresh
        </button>
      </div>

      {error && <p className="error-msg">{error}</p>}
      {loading && <p className="loading-msg">Loading…</p>}

      {!loading && dags.length === 0 && (
        <p className="empty-msg">No DAG runs found.</p>
      )}

      {dags.length > 0 && (
        <div className="card" style={{ padding: 0 }}>
          <table style={{ width: '100%' }}>
            <thead>
              <tr>
                <th>DAG Run ID</th>
                <th>Submitted at</th>
              </tr>
            </thead>
            <tbody>
              {dags.map((dag) => (
                <tr key={dag.dag_run_id}>
                  <td>
                    <Link to={`/dags/${dag.dag_run_id}`} className="monospace" style={{ fontSize: '0.85rem' }}>
                      {dag.dag_run_id}
                    </Link>
                  </td>
                  <td>{dag.submitted_at ? new Date(dag.submitted_at).toLocaleString() : '—'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {totalPages > 1 && (
        <div style={{ display: 'flex', gap: '0.5rem', alignItems: 'center', marginTop: '1rem' }}>
          <button
            className="btn btn-secondary"
            disabled={offset === 0}
            onClick={() => setOffset(Math.max(0, offset - PAGE_SIZE))}
          >
            ← Prev
          </button>
          <span style={{ color: '#666', fontSize: '0.9rem' }}>Page {page} of {totalPages}</span>
          <button
            className="btn btn-secondary"
            disabled={offset + PAGE_SIZE >= total}
            onClick={() => setOffset(offset + PAGE_SIZE)}
          >
            Next →
          </button>
        </div>
      )}
    </div>
  )
}
