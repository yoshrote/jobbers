import { useEffect, useState } from 'react'
import { getRegisteredTasks } from '../api/client'

/**
 * Renders a <select> populated from the registered task index when available,
 * falling back to a plain text <input>. Includes an "— any —" option so the
 * filter can be cleared.
 *
 * Props: value, onChange (receives the new string value)
 */
export default function TaskNameSelect({ value, onChange }) {
  const [taskTypes, setTaskTypes] = useState([])

  useEffect(() => {
    getRegisteredTasks().then((d) => setTaskTypes(d.tasks ?? [])).catch(() => {})
  }, [])

  const names = [...new Set(taskTypes.map(([name]) => name))]

  if (names.length === 0) {
    return (
      <input value={value} onChange={(e) => onChange(e.target.value)} placeholder="any" />
    )
  }

  return (
    <select value={value} onChange={(e) => onChange(e.target.value)}>
      <option value="">— any —</option>
      {names.map((name) => (
        <option key={name} value={name}>{name}</option>
      ))}
    </select>
  )
}
