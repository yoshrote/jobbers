import { render, screen } from '@testing-library/react'
import { vi } from 'vitest'
import App from '../src/App'
import * as client from '../src/api/client'

// Prevent real API calls from any page that runs on mount
vi.mock('../src/api/client')

beforeEach(() => {
  // Return a never-resolving promise so pages just show their loading state
  Object.values(client).forEach((fn) => {
    if (typeof fn === 'function') fn.mockReturnValue(new Promise(() => {}))
  })
})

afterEach(() => vi.clearAllMocks())

describe('App', () => {
  it('renders the navbar brand', () => {
    render(<App />)
    expect(screen.getByText('Jobbers')).toBeInTheDocument()
  })

  it('renders all expected navigation links', () => {
    render(<App />)
    const expectedLabels = [
      'Dashboard',
      'Active Tasks',
      'Queued Tasks',
      'Scheduled Tasks',
      'Dead Letter Queue',
      'Queue Configuration',
      'DAG Runs',
      'Cron DAGs',
      'Submit Task',
      'Submit DAG',
    ]
    for (const label of expectedLabels) {
      expect(screen.getByRole('link', { name: label })).toBeInTheDocument()
    }
  })

  it('renders the Dashboard route by default', () => {
    render(<App />)
    // The dashboard page renders a loading state while it fetches
    expect(screen.getByText(/loading/i)).toBeInTheDocument()
  })
})
