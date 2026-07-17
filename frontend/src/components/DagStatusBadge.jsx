/**
 * Renders a coloured pill for a DAG run's aggregate status string.
 * Matches the DagRunStatus StrEnum values from the backend
 * (running / complete / partial_failure / failed) -- distinct from per-task
 * TaskStatus values, so this uses its own dag-status-* colour classes.
 */
export default function DagStatusBadge({ status }) {
  const cls = `status-badge dag-status-${status?.toLowerCase() ?? 'running'}`
  return <span className={cls}>{status ?? 'unknown'}</span>
}
