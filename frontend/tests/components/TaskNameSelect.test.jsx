import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { vi } from 'vitest'
import TaskNameSelect from '../../src/components/TaskNameSelect'
import * as client from '../../src/api/client'

vi.mock('../../src/api/client')

describe('TaskNameSelect', () => {
  afterEach(() => vi.clearAllMocks())

  it('renders a plain text input when the API returns no tasks', async () => {
    client.getRegisteredTasks.mockResolvedValue({ tasks: [] })
    render(<TaskNameSelect value="" onChange={() => {}} />)
    // input appears immediately (tasks list is empty by default)
    expect(screen.getByRole('textbox')).toBeInTheDocument()
  })

  it('renders a select with options after the API resolves', async () => {
    client.getRegisteredTasks.mockResolvedValue({
      tasks: [['send_email', 1], ['process_payment', 2]],
    })
    render(<TaskNameSelect value="" onChange={() => {}} />)

    await waitFor(() => expect(screen.getByRole('combobox')).toBeInTheDocument())

    expect(screen.getByText('— any —')).toBeInTheDocument()
    expect(screen.getByText('send_email')).toBeInTheDocument()
    expect(screen.getByText('process_payment')).toBeInTheDocument()
  })

  it('deduplicates task names that appear in multiple versions', async () => {
    client.getRegisteredTasks.mockResolvedValue({
      tasks: [['my_task', 1], ['my_task', 2]],
    })
    render(<TaskNameSelect value="" onChange={() => {}} />)

    await waitFor(() => expect(screen.getByRole('combobox')).toBeInTheDocument())

    const options = screen.getAllByRole('option')
    expect(options.filter((o) => o.textContent === 'my_task')).toHaveLength(1)
  })

  it('falls back to input when the API call rejects', async () => {
    client.getRegisteredTasks.mockRejectedValue(new Error('network error'))
    render(<TaskNameSelect value="" onChange={() => {}} />)
    // Should still render an input (the error is swallowed)
    await waitFor(() => expect(screen.getByRole('textbox')).toBeInTheDocument())
  })

  it('calls onChange with the selected task name', async () => {
    const user = userEvent.setup()
    const onChange = vi.fn()
    client.getRegisteredTasks.mockResolvedValue({
      tasks: [['send_email', 1]],
    })
    render(<TaskNameSelect value="" onChange={onChange} />)

    await waitFor(() => expect(screen.getByRole('combobox')).toBeInTheDocument())

    await user.selectOptions(screen.getByRole('combobox'), 'send_email')
    expect(onChange).toHaveBeenCalledWith('send_email')
  })
})
