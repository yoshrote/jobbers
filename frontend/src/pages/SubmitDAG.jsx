import mermaid from 'mermaid'
import { useEffect, useRef, useState } from 'react'
import { Link } from 'react-router-dom'
import { submitDag } from '../api/client'

mermaid.initialize({ startOnLoad: false, theme: 'default' })

const PLACEHOLDER = `flowchart TD
    A["echo_task(value=hello)"]
    B["always_fail_task"]
    C["scheduled_fail_task"]
    D["echo_task(value=done)"]
    err["echo_task(value=error)"]

    A --> B
    A --> C
    B --> D
    C --> D
    B -.-> err`

export default function SubmitDAG() {
  const [diagram, setDiagram]     = useState(PLACEHOLDER)
  const [svgHtml, setSvgHtml]     = useState('')
  const [parseError, setParseError] = useState(null)
  const [result, setResult]       = useState(null)
  const [submitError, setSubmitError] = useState(null)
  const renderRef = useRef(null)
  const renderIdRef = useRef(0)

  // Re-render preview whenever diagram text changes (debounced).
  useEffect(() => {
    const id = ++renderIdRef.current
    const timer = setTimeout(async () => {
      if (id !== renderIdRef.current) return
      try {
        await mermaid.parse(diagram)
        setParseError(null)
        const { svg } = await mermaid.render(`dag-preview-${id}`, diagram)
        if (id === renderIdRef.current) setSvgHtml(svg)
      } catch (e) {
        setParseError(e.message ?? 'Syntax error')
        setSvgHtml('')
      }
    }, 400)
    return () => clearTimeout(timer)
  }, [diagram])

  async function handleSubmit(e) {
    e.preventDefault()
    setSubmitError(null)
    setResult(null)
    try {
      const res = await submitDag(diagram)
      setResult(res)
    } catch (e) {
      setSubmitError(e.message)
    }
  }

  return (
    <div>
      <h1>Submit DAG</h1>
      <p style={{ color: '#555', marginTop: 0 }}>
        Write a mermaid <code>flowchart TD</code> diagram to define a task dependency graph
        and submit it for execution.{' '}
        <a href="/docs/mermaid-dag-spec.md" target="_blank" rel="noreferrer">Spec reference ↗</a>
      </p>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1rem', alignItems: 'start' }}>
        {/* Editor */}
        <div className="card" style={{ margin: 0 }}>
          <h2 style={{ marginTop: 0 }}>Diagram</h2>
          <form onSubmit={handleSubmit}>
            <textarea
              value={diagram}
              onChange={(e) => setDiagram(e.target.value)}
              rows={20}
              style={{ fontFamily: 'monospace', fontSize: '0.85rem', width: '100%', boxSizing: 'border-box' }}
            />
            {parseError && <p className="error-msg">⚠ {parseError}</p>}
            {submitError && <p className="error-msg">{submitError}</p>}
            <button
              className="btn btn-primary"
              type="submit"
              disabled={!!parseError}
              style={{ marginTop: '0.5rem' }}
            >
              Submit DAG
            </button>
          </form>
        </div>

        {/* Preview */}
        <div className="card" style={{ margin: 0 }}>
          <h2 style={{ marginTop: 0 }}>Preview</h2>
          {svgHtml ? (
            <div ref={renderRef} dangerouslySetInnerHTML={{ __html: svgHtml }} />
          ) : (
            <p className="empty-msg">
              {parseError ? 'Fix the syntax error to see a preview.' : 'Loading…'}
            </p>
          )}
        </div>
      </div>

      {result && (
        <div className="card">
          <h2 style={{ color: 'green', marginTop: 0 }}>Submitted</h2>
          <p>DAG run: <Link to={`/dags/${result.dag_run_id}`} className="monospace">{result.dag_run_id}</Link></p>
          <p>Root task{result.root_task_ids.length > 1 ? 's' : ''}:</p>
          <ul>
            {result.root_task_ids.map((id) => (
              <li key={id}>
                <Link to={`/tasks/${id}`} className="monospace">{id}</Link>
              </li>
            ))}
          </ul>
        </div>
      )}
    </div>
  )
}
