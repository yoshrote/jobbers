import { render, screen } from '@testing-library/react'
import StatusBadge from '../../src/components/StatusBadge'

describe('StatusBadge', () => {
  it('renders the status text', () => {
    render(<StatusBadge status="COMPLETED" />)
    expect(screen.getByText('COMPLETED')).toBeInTheDocument()
  })

  it('applies a lowercase status class', () => {
    const { container } = render(<StatusBadge status="FAILED" />)
    expect(container.firstChild).toHaveClass('status-failed')
  })

  it('always applies the base status-badge class', () => {
    const { container } = render(<StatusBadge status="STARTED" />)
    expect(container.firstChild).toHaveClass('status-badge')
  })

  it('renders "unknown" and the unsubmitted class when status is undefined', () => {
    const { container } = render(<StatusBadge />)
    expect(screen.getByText('unknown')).toBeInTheDocument()
    expect(container.firstChild).toHaveClass('status-unsubmitted')
  })

  it.each([
    ['SUBMITTED'],
    ['STARTED'],
    ['COMPLETED'],
    ['FAILED'],
    ['CANCELLED'],
    ['STALLED'],
    ['SCHEDULED'],
  ])('renders status %s correctly', (status) => {
    const { container } = render(<StatusBadge status={status} />)
    expect(screen.getByText(status)).toBeInTheDocument()
    expect(container.firstChild).toHaveClass(`status-${status.toLowerCase()}`)
  })
})
