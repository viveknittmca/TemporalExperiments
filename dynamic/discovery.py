import importlib
import pkgutil
import inspect
import activities


def discover_activities(package):
    """Recursively find all activity functions in a package."""
    discovered_activities = []

    # Iterate over all modules in the package
    for _, module_name, is_pkg in pkgutil.walk_packages(package.__path__, package.__name__ + "."):
        if is_pkg:
            continue  # Skip sub-packages (handled recursively)

        # Import the module dynamically
        module = importlib.import_module(module_name)

        # Find all functions that are activities
        for _, func in inspect.getmembers(module, inspect.isfunction):
            # if hasattr(func, "_activity_definition"):  # ✅ Detect Temporal activities
            discovered_activities.append(func)

    return discovered_activities


if __name__ == '__main__':
    print(discover_activities(activities))
