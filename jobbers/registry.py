_task_function_map = {}

def register_task(name: str, version: int):
    """Register a task function with the given name and version."""
    # , max_concurrent=None, timeout=None, max_retries=3, retry_delay=None
    def decorator(func):
        """Decorate a task function and registers it for use with task instances."""
        if not callable(func):
            raise ValueError("Task function must be callable")
        if (name, version) in _task_function_map:
            raise ValueError(f"Task {name} version {version} is already registered")
        _task_function_map[(name, version)] = func
        return func
    return decorator

def get_task_function(name: str, version: int):
    """Retrieve a task function given its name."""
    return _task_function_map.get((name, version))
