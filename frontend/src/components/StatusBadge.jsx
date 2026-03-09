/**
 * Renders a coloured pill for a task status string.
 * Matches the TaskStatus StrEnum values from the backend.
 */
export default function StatusBadge({ status }) {
  const cls = `status-badge status-${status?.toLowerCase() ?? 'unsubmitted'}`
  return <span className={cls}>{status ?? 'unknown'}</span>
}
