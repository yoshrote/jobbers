_task_function_map = {}

def register_task(name: str, version: int):
    """Register a task function with the given name."""
    def decorator(func):
        _task_function_map[(name, version)] = func
        return func
    return decorator

def get_task_function(name: str, version: int):
    """Retrieve a task function given its name."""
    return _task_function_map.get((name, version))
