import { useEffect, useState } from 'react'
import { getAllQueues, getQueuesForRole, getRoles, setQueuesForRole } from '../api/client'

export default function Queues() {
  const [roles,     setRoles]     = useState([])
  const [allQueues, setAllQueues] = useState({})
  const [error,     setError]     = useState(null)

  // Per-role editing state
  const [editRole,   setEditRole]   = useState('')
  const [roleQueues, setRoleQueues] = useState([])
  const [editInput,  setEditInput]  = useState('')
  const [saveMsg,    setSaveMsg]    = useState(null)

  useEffect(() => {
    Promise.all([getRoles(), getAllQueues()])
      .then(([r, q]) => { setRoles(r.roles ?? []); setAllQueues(q.queues ?? {}) })
      .catch((e) => setError(e.message))
  }, [])

  async function selectRole(role) {
    setEditRole(role)
    setSaveMsg(null)
    try {
      const res = await getQueuesForRole(role)
      const qs = res.queues ?? []
      setRoleQueues(qs)
      setEditInput(qs.join('\n'))
    } catch (e) {
      setError(e.message)
    }
  }

  async function handleSave() {
    const queues = editInput.split('\n').map((q) => q.trim()).filter(Boolean)
    try {
      await setQueuesForRole(editRole, queues)
      setSaveMsg('Saved.')
      setRoleQueues(queues)
      // Refresh summary
      getAllQueues().then((q) => setAllQueues(q.queues ?? {})).catch(() => {})
    } catch (e) {
      setError(e.message)
    }
  }

  return (
    <div>
      <h1>Queues</h1>
      {error && <p className="error-msg">{error}</p>}

      <div style={{ display: 'grid', gridTemplateColumns: '220px 1fr', gap: '1rem' }}>
        {/* Role list */}
        <div className="card" style={{ padding: '.75rem' }}>
          <h2 style={{ marginTop: 0, fontSize: '0.95rem' }}>Roles</h2>
          {roles.length === 0 && <p className="empty-msg">No roles found.</p>}
          <ul style={{ listStyle: 'none', padding: 0, margin: 0 }}>
            {roles.map((r) => (
              <li key={r}>
                <button
                  className={`btn btn-sm ${editRole === r ? 'btn-primary' : 'btn-secondary'}`}
                  style={{ width: '100%', textAlign: 'left', marginBottom: 4 }}
                  onClick={() => selectRole(r)}
                >
                  {r}
                </button>
              </li>
            ))}
          </ul>
        </div>

        {/* Queue editor */}
        <div className="card">
          {!editRole ? (
            <p className="empty-msg">Select a role to view and edit its queues.</p>
          ) : (
            <>
              <h2 style={{ marginTop: 0 }}>Queues for <em>{editRole}</em></h2>
              <div className="form-row">
                <label>One queue name per line</label>
                <textarea
                  value={editInput}
                  onChange={(e) => setEditInput(e.target.value)}
                  rows={Math.max(6, roleQueues.length + 2)}
                />
              </div>
              {saveMsg && <p style={{ color: 'green' }}>{saveMsg}</p>}
              <button className="btn btn-primary" onClick={handleSave}>Save</button>
            </>
          )}
        </div>
      </div>

      {/* All queues summary */}
      {Object.keys(allQueues).length > 0 && (
        <div className="card">
          <h2>All queues by role</h2>
          <table>
            <thead>
              <tr><th>Role</th><th>Queues</th></tr>
            </thead>
            <tbody>
              {Object.entries(allQueues).map(([role, qs]) => (
                <tr key={role}>
                  <td><strong>{role}</strong></td>
                  <td>{Array.isArray(qs) ? qs.join(', ') : JSON.stringify(qs)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
