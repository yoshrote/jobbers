import { useEffect, useState } from 'react'
import { getIndex, submitTask } from '../api/client'
import { ulid } from "ulid";

// Generate a ULID-like placeholder for the id field.
// In production you'd use a real ULID library; this is just a UI hint.
function tempId() {
  return ulid()
}

export default function SubmitTask() {
  const [taskTypes, setTaskTypes] = useState([])
  const [selectedTask, setSelectedTask] = useState('')
  const [form, setForm] = useState({
    id: tempId(),
    name: '',
    queue: 'default',
    version: 0,
    parameters: '{}',
  })
  const [result, setResult] = useState(null)
  const [error,  setError]  = useState(null)

  useEffect(() => {
    getIndex().then((d) => setTaskTypes(d.tasks ?? [])).catch(() => {})
  }, [])

  function set(field) {
    return (e) => setForm((f) => ({ ...f, [field]: e.target.value }))
  }

  async function handleSubmit(e) {
    e.preventDefault()
    setError(null)
    setResult(null)

    let parameters
    try {
      parameters = JSON.parse(form.parameters)
    } catch {
      setError('Parameters must be valid JSON.')
      return
    }

    try {
      const res = await submitTask({
        id: form.id,
        name: form.name,
        queue: form.queue,
        version: Number(form.version),
        parameters,
      })
      setResult(res)
      setForm((f) => ({ ...f, id: tempId() }))
    } catch (e) {
      setError(e.message)
    }
  }

  return (
    <div>
      <h1>Submit task</h1>
      <div className="card" style={{ maxWidth: 560 }}>
        <form onSubmit={handleSubmit}>
          <div className="form-row">
            <label>Task ID (ULID)</label>
            <input value={form.id} onChange={set('id')} required className="monospace" />
          </div>

          <div className="form-row">
            <label>Task name</label>
            {taskTypes.length > 0 ? (
              <select
                value={selectedTask}
                onChange={(e) => {
                  const val = e.target.value
                  setSelectedTask(val)
                  const [name, version] = val.split('\0')
                  setForm((f) => ({ ...f, name, version: Number(version) }))
                }}
                required
              >
                <option value="">— select —</option>
                {taskTypes.map(([name, version]) => (
                  <option key={`${name}\0${version}`} value={`${name}\0${version}`}>
                    {name} (v{version})
                  </option>
                ))}
              </select>
            ) : (
              <input value={form.name} onChange={set('name')} required placeholder="my_task" />
            )}
          </div>

          <div className="form-row">
            <label>Queue</label>
            <input value={form.queue} onChange={set('queue')} required placeholder="default" />
          </div>

          <input type="hidden" value={form.version} />

          <div className="form-row">
            <label>Parameters (JSON)</label>
            <textarea value={form.parameters} onChange={set('parameters')} rows={5} />
          </div>

          {error  && <p className="error-msg">{error}</p>}

          <button className="btn btn-primary" type="submit">Submit</button>
        </form>
      </div>

      {result && (
        <div className="card" style={{ maxWidth: 560 }}>
          <h2 style={{ color: 'green', marginTop: 0 }}>Submitted</h2>
          <pre>{JSON.stringify(result, null, 2)}</pre>
        </div>
      )}
    </div>
  )
}
