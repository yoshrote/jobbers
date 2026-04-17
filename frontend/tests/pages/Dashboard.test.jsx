import { render, screen, waitFor } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { vi } from 'vitest'
import Dashboard from '../../src/pages/Dashboard'
import * as client from '../../src/api/client'

vi.mock('../../src/api/client')

function renderDashboard() {
  return render(
    <MemoryRouter>
      <Dashboard />
    </MemoryRouter>
  )
}

describe('Dashboard', () => {
  afterEach(() => vi.clearAllMocks())

  it('shows a loading message while data is pending', () => {
    // Never-resolving promise keeps the component in loading state
    client.getRegisteredTasks.mockReturnValue(new Promise(() => {}))
    client.getAllQueues.mockReturnValue(new Promise(() => {}))
    client.getRoles.mockReturnValue(new Promise(() => {}))

    renderDashboard()
    expect(screen.getByText(/loading/i)).toBeInTheDocument()
  })

  it('shows an error message when the API call fails', async () => {
    client.getRegisteredTasks.mockRejectedValue(new Error('connection refused'))
    client.getAllQueues.mockRejectedValue(new Error('connection refused'))
    client.getRoles.mockRejectedValue(new Error('connection refused'))

    renderDashboard()
    await waitFor(() => expect(screen.getByText(/connection refused/i)).toBeInTheDocument())
  })

  it('renders task type count, role count, and queue count on success', async () => {
    client.getRegisteredTasks.mockResolvedValue({ tasks: [['task_a', 1], ['task_b', 1]] })
    client.getAllQueues.mockResolvedValue({ queues: ['default', 'priority'] })
    client.getRoles.mockResolvedValue({ roles: ['worker', 'scheduler'] })

    renderDashboard()

    await waitFor(() => expect(screen.getByText('Dashboard')).toBeInTheDocument())

    // Verify stat labels — "Registered task types" also appears as an h2,
    // so use getAllByText and confirm it appears at least once.
    expect(screen.getAllByText('Registered task types').length).toBeGreaterThanOrEqual(1)
    expect(screen.getByText('Roles')).toBeInTheDocument()
    expect(screen.getByText('Total queues')).toBeInTheDocument()
    // All three stat values should be 2
    expect(screen.getAllByText('2')).toHaveLength(3)
  })

  it('renders registered task names as links', async () => {
    client.getRegisteredTasks.mockResolvedValue({ tasks: [['my_task', 1]] })
    client.getAllQueues.mockResolvedValue({ queues: [] })
    client.getRoles.mockResolvedValue({ roles: [] })

    renderDashboard()

    // The component renders task entries ([name, version]) directly as JSX children,
    // so the link text is "my_task1". Match with a regex and verify the href.
    await waitFor(() =>
      expect(screen.getByRole('link', { name: /my_task/ })).toBeInTheDocument()
    )
    expect(screen.getByRole('link', { name: /my_task/ })).toHaveAttribute(
      'href',
      expect.stringContaining('my_task')
    )
  })

  it('shows empty message when no task types are registered', async () => {
    client.getRegisteredTasks.mockResolvedValue({ tasks: [] })
    client.getAllQueues.mockResolvedValue({ queues: [] })
    client.getRoles.mockResolvedValue({ roles: [] })

    renderDashboard()

    await waitFor(() => expect(screen.getByText(/no task types registered/i)).toBeInTheDocument())
  })
})
