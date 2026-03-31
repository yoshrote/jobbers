import mermaid from 'mermaid'
import { useEffect, useRef, useState } from 'react'
import { createCronDag, deleteCronDag, getCronDags, updateCronDag } from '../api/client'

mermaid.initialize({ startOnLoad: false, theme: 'default' })

const EMPTY_FORM = {
  name: '',
  cron_expr: '',
  diagram: 'flowchart TD\n    A["task_name:queue(key=val)"]',
  enabled: true,
  concurrency_policy: 'always',
}

function MermaidDiagram({ diagram, id }) {
  const [svg, setSvg] = useState('')
  useEffect(() => {
    let cancelled = false
    mermaid.render(`cron-dag-${id}`, diagram)
      .then(({ svg }) => { if (!cancelled) setSvg(svg) })
      .catch(() => { if (!cancelled) setSvg('') })
    return () => { cancelled = true }
  }, [diagram, id])
  if (!svg) return null
  return <div dangerouslySetInnerHTML={{ __html: svg }} style={{ maxWidth: 480 }} />
}

function CronDAGEditor({ initial, onSave, onCancel }) {
  const [form, setForm] = useState(initial)
  const [error, setError] = useState(null)
  const [previewSvg, setPreviewSvg] = useState('')
  const [parseError, setParseError] = useState(null)
  const renderIdRef = useRef(0)

  useEffect(() => {
    const id = ++renderIdRef.current
    const timer = setTimeout(async () => {
      if (id !== renderIdRef.current) return
      try {
        await mermaid.parse(form.diagram)
        setParseError(null)
        const { svg } = await mermaid.render(`editor-preview-${id}`, form.diagram)
        if (id === renderIdRef.current) setPreviewSvg(svg)
      } catch (e) {
        setParseError(e.message ?? 'Syntax error')
        setPreviewSvg('')
      }
    }, 400)
    return () => clearTimeout(timer)
  }, [form.diagram])

  function set(field) {
    return (e) => {
      const value = e.target.type === 'checkbox' ? e.target.checked : e.target.value
      setForm((f) => ({ ...f, [field]: value }))
    }
  }

  async function handleSubmit(e) {
    e.preventDefault()
    setError(null)
    try {
      await onSave(form)
    } catch (e) {
      setError(e.message)
    }
  }

  return (
    <div className="card" style={{ maxWidth: 900 }}>
      <form onSubmit={handleSubmit}>
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1rem' }}>
          <div>
            <div className="form-row">
              <label>Name</label>
              <input value={form.name} onChange={set('name')} required placeholder="daily_report" />
            </div>
            <div className="form-row">
              <label>Cron expression</label>
              <input value={form.cron_expr} onChange={set('cron_expr')} required placeholder="0 6 * * *" className="monospace" />
            </div>
            <div className="form-row">
              <label>Concurrency policy</label>
              <select value={form.concurrency_policy} onChange={set('concurrency_policy')}>
                <option value="always">always</option>
                <option value="skip_if_running">skip_if_running</option>
              </select>
            </div>
            <div className="form-row">
              <label style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                <input type="checkbox" checked={form.enabled} onChange={set('enabled')} />
                Enabled
              </label>
            </div>
            <div className="form-row">
              <label>Diagram</label>
              <textarea
                value={form.diagram}
                onChange={set('diagram')}
                rows={12}
                style={{ fontFamily: 'monospace', fontSize: '0.82rem', width: '100%', boxSizing: 'border-box' }}
              />
              {parseError && <p className="error-msg" style={{ marginTop: '0.25rem' }}>⚠ {parseError}</p>}
            </div>
          </div>
          <div>
            <p style={{ marginTop: 0, fontWeight: 600 }}>Preview</p>
            {previewSvg
              ? <div dangerouslySetInnerHTML={{ __html: previewSvg }} />
              : <p className="empty-msg">{parseError ? 'Fix syntax error to preview.' : 'Loading…'}</p>
            }
          </div>
        </div>

        {error && <p className="error-msg">{error}</p>}
        <div style={{ display: 'flex', gap: '0.5rem', marginTop: '0.5rem' }}>
          <button className="btn btn-primary" type="submit" disabled={!!parseError}>Save</button>
          <button className="btn btn-secondary" type="button" onClick={onCancel}>Cancel</button>
        </div>
      </form>
    </div>
  )
}

export default function CronDags() {
  const [entries, setEntries] = useState([])
  const [total, setTotal]     = useState(0)
  const [loading, setLoading] = useState(true)
  const [error, setError]     = useState(null)
  const [editing, setEditing] = useState(null)   // null | 'new' | entry object
  const [expandedId, setExpandedId] = useState(null)

  function load() {
    setLoading(true)
    setError(null)
    getCronDags()
      .then((d) => { setEntries(d.cron_dags ?? []); setTotal(d.total ?? 0) })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false))
  }

  useEffect(() => { load() }, [])

  async function handleCreate(form) {
    await createCronDag(form)
    setEditing(null)
    load()
  }

  async function handleUpdate(entry, form) {
    await updateCronDag(entry.id, form)
    setEditing(null)
    load()
  }

  async function handleDelete(entry) {
    if (!window.confirm(`Delete cron DAG "${entry.name}"?`)) return
    try {
      await deleteCronDag(entry.id)
      load()
    } catch (e) {
      setError(e.message)
    }
  }

  if (loading) return <p className="loading-msg">Loading…</p>

  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'center', gap: '1rem', marginBottom: '1rem' }}>
        <h1 style={{ margin: 0 }}>Cron DAGs</h1>
        <span style={{ color: '#666' }}>{total} total</span>
        <button className="btn btn-primary" onClick={() => setEditing('new')} style={{ marginLeft: 'auto' }}>
          + New cron DAG
        </button>
      </div>

      {error && <p className="error-msg">{error}</p>}

      {editing === 'new' && (
        <CronDAGEditor
          initial={EMPTY_FORM}
          onSave={handleCreate}
          onCancel={() => setEditing(null)}
        />
      )}

      {entries.length === 0 && !loading && (
        <p className="empty-msg">No cron DAG entries yet.</p>
      )}

      <div style={{ display: 'flex', flexDirection: 'column', gap: '0.75rem' }}>
        {entries.map((entry) => (
          <div key={entry.id}>
            {editing && editing !== 'new' && editing.id === entry.id ? (
              <CronDAGEditor
                initial={{
                  name: entry.name,
                  cron_expr: entry.cron_expr,
                  diagram: entry.diagram,
                  enabled: entry.enabled,
                  concurrency_policy: entry.concurrency_policy,
                }}
                onSave={(form) => handleUpdate(entry, form)}
                onCancel={() => setEditing(null)}
              />
            ) : (
              <div className="card" style={{ margin: 0 }}>
                <div style={{ display: 'flex', alignItems: 'flex-start', gap: '1rem' }}>
                  <div style={{ flex: 1 }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
                      <strong>{entry.name}</strong>
                      <code className="monospace" style={{ fontSize: '0.85rem', background: '#f0f0f0', padding: '1px 6px', borderRadius: 3 }}>
                        {entry.cron_expr}
                      </code>
                      <span style={{ color: entry.enabled ? 'green' : '#999', fontSize: '0.85rem' }}>
                        {entry.enabled ? 'enabled' : 'disabled'}
                      </span>
                    </div>
                    <div style={{ color: '#666', fontSize: '0.85rem', marginTop: '0.25rem' }}>
                      Next run: {entry.next_run_at ? new Date(entry.next_run_at).toLocaleString() : '—'}
                      {' · '}concurrency: {entry.concurrency_policy}
                      {' · '}id: <span className="monospace">{entry.id}</span>
                    </div>
                  </div>
                  <div style={{ display: 'flex', gap: '0.5rem' }}>
                    <button
                      className="btn btn-secondary"
                      onClick={() => setExpandedId(expandedId === entry.id ? null : entry.id)}
                    >
                      {expandedId === entry.id ? 'Hide' : 'Diagram'}
                    </button>
                    <button className="btn btn-secondary" onClick={() => setEditing(entry)}>Edit</button>
                    <button className="btn btn-danger" onClick={() => handleDelete(entry)}>Delete</button>
                  </div>
                </div>
                {expandedId === entry.id && (
                  <div style={{ marginTop: '1rem', borderTop: '1px solid #eee', paddingTop: '1rem' }}>
                    <MermaidDiagram diagram={entry.diagram} id={entry.id} />
                  </div>
                )}
              </div>
            )}
          </div>
        ))}
      </div>
    </div>
  )
}
