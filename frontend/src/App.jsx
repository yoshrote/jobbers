import { BrowserRouter, NavLink, Route, Routes } from 'react-router-dom'
import Dashboard from './pages/Dashboard'
import DeadLetterQueue from './pages/DeadLetterQueue'
import Queues from './pages/Queues'
import ScheduledTasks from './pages/ScheduledTasks'
import SubmitTask from './pages/SubmitTask'
import TaskDetail from './pages/TaskDetail'
import TaskList from './pages/TaskList'

const NAV_LINKS = [
  { to: '/', label: 'Dashboard' },
  { to: '/tasks', label: 'Tasks' },
  { to: '/scheduled', label: 'Scheduled' },
  { to: '/dlq', label: 'Dead Letter Queue' },
  { to: '/queues', label: 'Queues' },
  { to: '/submit', label: 'Submit Task' },
]

export default function App() {
  return (
    <BrowserRouter>
      <nav className="navbar">
        <span className="navbar-brand">Jobbers</span>
        <ul className="navbar-links">
          {NAV_LINKS.map(({ to, label }) => (
            <li key={to}>
              <NavLink
                to={to}
                end={to === '/'}
                className={({ isActive }) => isActive ? 'active' : undefined}
              >
                {label}
              </NavLink>
            </li>
          ))}
        </ul>
      </nav>

      <main className="main-content">
        <Routes>
          <Route path="/" element={<Dashboard />} />
          <Route path="/tasks" element={<TaskList />} />
          <Route path="/tasks/:taskId" element={<TaskDetail />} />
          <Route path="/scheduled" element={<ScheduledTasks />} />
          <Route path="/dlq" element={<DeadLetterQueue />} />
          <Route path="/queues" element={<Queues />} />
          <Route path="/submit" element={<SubmitTask />} />
        </Routes>
      </main>
    </BrowserRouter>
  )
}
