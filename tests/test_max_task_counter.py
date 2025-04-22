from jobbers.task_generator import MaxTaskCounter


def test_max_task_counter_initial_state():
    """Test that MaxTaskCounter initializes correctly."""
    counter = MaxTaskCounter(max_tasks=5)
    assert counter.max_tasks == 5, "MaxTaskCounter should initialize with the correct max_tasks value"
    assert counter._task_count == 0, "Task count should start at 0"

def test_max_task_counter_limit_not_reached():
    """Test that limit_reached returns False when the limit is not reached."""
    counter = MaxTaskCounter(max_tasks=5)
    assert not counter.limit_reached(), "limit_reached should return False when task count is below max_tasks"

def test_max_task_counter_limit_reached():
    """Test that limit_reached returns True when the limit is reached."""
    counter = MaxTaskCounter(max_tasks=5)
    counter._task_count = 5
    assert counter.limit_reached(), "limit_reached should return True when task count equals max_tasks"

def test_max_task_counter_increment_task_count():
    """Test that the task count increments correctly when used as a context manager."""
    counter = MaxTaskCounter(max_tasks=5)
    with counter:
        pass  # Simulate processing a task
    assert counter._task_count == 1, "Task count should increment by 1 after exiting the context manager"

def test_max_task_counter_no_limit():
    """Test that MaxTaskCounter works correctly when max_tasks is 0 (no limit)."""
    counter = MaxTaskCounter(max_tasks=0)
    assert not counter.limit_reached(), "limit_reached should always return False when max_tasks is 0"
    with counter:
        pass  # Simulate processing a task
    assert counter._task_count == 0, "Task count should not increment when no limit is set"
