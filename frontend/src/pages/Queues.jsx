import { useEffect, useState } from 'react'
import {
  getAllQueues, getRoles, getRole,
  createQueue, getQueueConfig, updateQueue, deleteQueue,
  createRole, updateRole, deleteRole,
} from '../api/client'

const RATE_PERIODS = ['', 'second', 'minute', 'hour', 'day']
const emptyQueueForm = { name: '', max_concurrent: '10', rate_numerator: '', rate_denominator: '', rate_period: '' }

export default function Queues() {
  // ── data ──
  const [roles, setRoles]               = useState([])
  const [allQueueNames, setAllQueueNames] = useState([])
  const [queueConfigs, setQueueConfigs] = useState({}) // name → config | null
  const [error, setError]               = useState(null)

  // ── role editing ──
  const [editRole, setEditRole]     = useState('')
  const [roleQueues, setRoleQueues] = useState([])
  const [editInput, setEditInput]   = useState('')
  const [roleMsg, setRoleMsg]       = useState(null)

  // ── new role form ──
  const [showNewRole, setShowNewRole]     = useState(false)
  const [newRoleName, setNewRoleName]     = useState('')
  const [newRoleQueues, setNewRoleQueues] = useState('')
  const [newRoleError, setNewRoleError]   = useState(null)

  // ── queue config editing: null = closed, '' = new, else queue name ──
  const [editQueue, setEditQueue] = useState(null)
  const [queueForm, setQueueForm] = useState(emptyQueueForm)
  const [queueError, setQueueError] = useState(null)

  useEffect(() => { loadAll() }, [])

  async function loadAll() {
    setError(null)
    try {
      const [r, q] = await Promise.all([getRoles(), getAllQueues()])
      const rolesList  = r.roles  ?? []
      const queueNames = q.queues ?? []
      setRoles(rolesList)
      setAllQueueNames(queueNames)
      await loadQueueConfigs(queueNames)
    } catch (e) {
      setError(e.message)
    }
  }

  async function loadQueueConfigs(names) {
    if (names.length === 0) { setQueueConfigs({}); return }
    const results = await Promise.all(
      names.map(n => getQueueConfig(n).then(r => r.queue).catch(() => null))
    )
    const map = {}
    names.forEach((n, i) => { map[n] = results[i] })
    setQueueConfigs(map)
  }

  // ── role handlers ─────────────────────────────────────────────────────────

  async function selectRole(role) {
    setEditRole(role)
    setRoleMsg(null)
    try {
      const res = await getRole(role)
      const qs = res.queues ?? []
      setRoleQueues(qs)
      setEditInput(qs.join('\n'))
    } catch (e) {
      setError(e.message)
    }
  }

  async function handleSaveRole() {
    const queues = editInput.split('\n').map(q => q.trim()).filter(Boolean)
    try {
      await updateRole(editRole, queues)
      setRoleMsg('Saved.')
      setRoleQueues(queues)
    } catch (e) {
      setError(e.message)
    }
  }

  async function handleDeleteRole(role) {
    if (!confirm(`Delete role "${role}"? Its queue configs will be preserved.`)) return
    try {
      await deleteRole(role)
      const updated = roles.filter(r => r !== role)
      setRoles(updated)
      if (editRole === role) { setEditRole(''); setRoleQueues([]); setEditInput('') }
    } catch (e) {
      setError(e.message)
    }
  }

  async function handleCreateRole() {
    setNewRoleError(null)
    if (!newRoleName.trim()) { setNewRoleError('Role name is required.'); return }
    const queues = newRoleQueues.split('\n').map(q => q.trim()).filter(Boolean)
    try {
      await createRole(newRoleName.trim(), queues)
      setShowNewRole(false)
      setNewRoleName('')
      setNewRoleQueues('')
      await loadAll()
    } catch (e) {
      setNewRoleError(e.message)
    }
  }

  // ── queue config handlers ─────────────────────────────────────────────────

  function startEditQueue(name) {
    const cfg = queueConfigs[name]
    setEditQueue(name)
    setQueueError(null)
    setQueueForm({
      name,
      max_concurrent:  cfg?.max_concurrent  ?? '',
      rate_numerator:  cfg?.rate_numerator  ?? '',
      rate_denominator: cfg?.rate_denominator ?? '',
      rate_period:     cfg?.rate_period     ?? '',
    })
  }

  function startNewQueue() {
    setEditQueue('')
    setQueueError(null)
    setQueueForm(emptyQueueForm)
  }

  async function handleSaveQueue() {
    setQueueError(null)
    const isNew = editQueue === ''
    if (isNew && !queueForm.name.trim()) { setQueueError('Queue name is required.'); return }
    const body = {
      name:             isNew ? queueForm.name.trim() : editQueue,
      max_concurrent:   queueForm.max_concurrent  !== '' ? Number(queueForm.max_concurrent)  : null,
      rate_numerator:   queueForm.rate_numerator  !== '' ? Number(queueForm.rate_numerator)  : null,
      rate_denominator: queueForm.rate_denominator !== '' ? Number(queueForm.rate_denominator) : null,
      rate_period:      queueForm.rate_period || null,
    }
    try {
      if (isNew) {
        await createQueue(body)
      } else {
        await updateQueue(editQueue, body)
      }
      setEditQueue(null)
      await loadAll()
    } catch (e) {
      setQueueError(e.message)
    }
  }

  async function handleDeleteQueue(name) {
    if (!confirm(`Delete queue "${name}"? It will be removed from all roles.`)) return
    try {
      await deleteQueue(name)
      await loadAll()
      if (editQueue === name) setEditQueue(null)
    } catch (e) {
      setError(e.message)
    }
  }

  // ── helpers ───────────────────────────────────────────────────────────────

  function rateLabel(cfg) {
    if (!cfg?.rate_numerator || !cfg?.rate_period) return '—'
    return `${cfg.rate_numerator} per ${cfg.rate_denominator} ${cfg.rate_period}`
  }

  function updateForm(field, value) {
    setQueueForm(f => ({ ...f, [field]: value }))
  }

  // ── render ────────────────────────────────────────────────────────────────

  return (
    <div>
      <h1>Queues &amp; Roles</h1>
      {error && <p className="error-msg">{error}</p>}

      {/* ── Queue Configs ── */}
      <div className="card">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '.75rem' }}>
          <h2 style={{ margin: 0 }}>Queue Configs</h2>
          {editQueue === null && (
            <button className="btn btn-primary btn-sm" onClick={startNewQueue}>+ New Queue</button>
          )}
        </div>

        {/* inline create / edit form */}
        {editQueue !== null && (
          <div style={{ background: '#f9f9f9', border: '1px solid #d0d0d0', borderRadius: 4, padding: '1rem', marginBottom: '1rem' }}>
            <h3 style={{ marginTop: 0, fontSize: '1rem' }}>
              {editQueue === '' ? 'New Queue' : `Edit: ${editQueue}`}
            </h3>

            {editQueue === '' && (
              <div className="form-row">
                <label>Queue Name</label>
                <input
                  value={queueForm.name}
                  onChange={e => updateForm('name', e.target.value)}
                  placeholder="my-queue"
                  style={{ maxWidth: 280 }}
                />
              </div>
            )}

            <div className="form-row" style={{ maxWidth: 280 }}>
              <label>Max Concurrent</label>
              <input
                type="number"
                min="1"
                value={queueForm.max_concurrent}
                onChange={e => updateForm('max_concurrent', e.target.value)}
                placeholder="10"
              />
            </div>

            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: '.5rem', maxWidth: 480 }}>
              <div className="form-row">
                <label>Rate (tasks)</label>
                <input
                  type="number"
                  min="1"
                  value={queueForm.rate_numerator}
                  onChange={e => updateForm('rate_numerator', e.target.value)}
                  placeholder="e.g. 100"
                />
              </div>
              <div className="form-row">
                <label>Per (N)</label>
                <input
                  type="number"
                  min="1"
                  value={queueForm.rate_denominator}
                  onChange={e => updateForm('rate_denominator', e.target.value)}
                  placeholder="e.g. 1"
                />
              </div>
              <div className="form-row">
                <label>Period</label>
                <select value={queueForm.rate_period} onChange={e => updateForm('rate_period', e.target.value)}>
                  {RATE_PERIODS.map(p => <option key={p} value={p}>{p || '—'}</option>)}
                </select>
              </div>
            </div>

            {queueError && <p className="error-msg" style={{ marginBottom: '.5rem' }}>{queueError}</p>}

            <div style={{ display: 'flex', gap: '.5rem' }}>
              <button className="btn btn-primary btn-sm" onClick={handleSaveQueue}>
                {editQueue === '' ? 'Create' : 'Save'}
              </button>
              <button className="btn btn-secondary btn-sm" onClick={() => setEditQueue(null)}>Cancel</button>
            </div>
          </div>
        )}

        {allQueueNames.length === 0 ? (
          <p className="empty-msg">No queues configured.</p>
        ) : (
          <table>
            <thead>
              <tr>
                <th>Name</th>
                <th>Max Concurrent</th>
                <th>Rate Limit</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              {allQueueNames.map(name => {
                const cfg = queueConfigs[name]
                return (
                  <tr key={name}>
                    <td><strong>{name}</strong></td>
                    <td>
                      {cfg
                        ? cfg.max_concurrent ?? <em style={{ color: '#888' }}>unlimited</em>
                        : <span style={{ color: '#aaa' }}>10 (default)</span>}
                    </td>
                    <td>
                      {cfg
                        ? rateLabel(cfg)
                        : <span style={{ color: '#aaa' }}>— (default)</span>}
                    </td>
                    <td style={{ whiteSpace: 'nowrap' }}>
                      <button
                        className="btn btn-sm btn-secondary"
                        style={{ marginRight: 4 }}
                        onClick={() => startEditQueue(name)}
                        disabled={editQueue !== null}
                      >
                        Edit
                      </button>
                      <button
                        className="btn btn-sm btn-danger"
                        onClick={() => handleDeleteQueue(name)}
                      >
                        Delete
                      </button>
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        )}
      </div>

      {/* ── Roles ── */}
      <div className="card">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '.75rem' }}>
          <h2 style={{ margin: 0 }}>Roles</h2>
          {!showNewRole && (
            <button className="btn btn-primary btn-sm" onClick={() => { setShowNewRole(true); setNewRoleError(null) }}>
              + New Role
            </button>
          )}
        </div>

        {/* new role form */}
        {showNewRole && (
          <div style={{ background: '#f9f9f9', border: '1px solid #d0d0d0', borderRadius: 4, padding: '.75rem', marginBottom: '.75rem' }}>
            <h3 style={{ marginTop: 0, fontSize: '1rem' }}>New Role</h3>
            <div className="form-row" style={{ maxWidth: 280 }}>
              <label>Role Name</label>
              <input
                value={newRoleName}
                onChange={e => setNewRoleName(e.target.value)}
                placeholder="worker-role"
              />
            </div>
            <div className="form-row">
              <label>Queues (one per line)</label>
              <textarea
                value={newRoleQueues}
                onChange={e => setNewRoleQueues(e.target.value)}
                rows={3}
                placeholder="queue-name"
              />
            </div>
            {newRoleError && <p className="error-msg" style={{ marginBottom: '.5rem' }}>{newRoleError}</p>}
            <div style={{ display: 'flex', gap: '.5rem' }}>
              <button className="btn btn-primary btn-sm" onClick={handleCreateRole}>Create</button>
              <button
                className="btn btn-secondary btn-sm"
                onClick={() => { setShowNewRole(false); setNewRoleName(''); setNewRoleQueues(''); setNewRoleError(null) }}
              >
                Cancel
              </button>
            </div>
          </div>
        )}

        <div style={{ display: 'grid', gridTemplateColumns: '220px 1fr', gap: '1rem' }}>
          {/* role list */}
          <div style={{ borderRight: '1px solid #eee', paddingRight: '1rem' }}>
            {roles.length === 0 ? (
              <p className="empty-msg">No roles.</p>
            ) : (
              <ul style={{ listStyle: 'none', padding: 0, margin: 0 }}>
                {roles.map(r => (
                  <li key={r} style={{ display: 'flex', alignItems: 'center', gap: 4, marginBottom: 4 }}>
                    <button
                      className={`btn btn-sm ${editRole === r ? 'btn-primary' : 'btn-secondary'}`}
                      style={{ flex: 1, textAlign: 'left' }}
                      onClick={() => selectRole(r)}
                    >
                      {r}
                    </button>
                    <button
                      className="btn btn-sm btn-danger"
                      title="Delete role"
                      onClick={() => handleDeleteRole(r)}
                    >
                      ×
                    </button>
                  </li>
                ))}
              </ul>
            )}
          </div>

          {/* queue editor for selected role */}
          <div>
            {!editRole ? (
              <p className="empty-msg">Select a role to view and edit its queues.</p>
            ) : (
              <>
                <h3 style={{ marginTop: 0 }}>Queues for <em>{editRole}</em></h3>
                <div className="form-row">
                  <label>One queue name per line</label>
                  <textarea
                    value={editInput}
                    onChange={e => setEditInput(e.target.value)}
                    rows={Math.max(5, roleQueues.length + 2)}
                  />
                </div>
                {roleMsg && <p style={{ color: 'green', margin: '4px 0 8px' }}>{roleMsg}</p>}
                <button className="btn btn-primary btn-sm" onClick={handleSaveRole}>Save</button>
              </>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}
