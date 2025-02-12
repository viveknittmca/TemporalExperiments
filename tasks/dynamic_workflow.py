import pkgutil
import importlib
import inspect
from temporalio import workflow
from datetime import timedelta
from Base_task import BaseTask
from typing import List

def discover_Base_tasks(package):
    """Dynamically discover all classes that inherit from BaseTask."""
    discovered_tasks = {}

    for _, module_name, _ in pkgutil.walk_packages(package.__path__, package.__name__ + "."):
        module = importlib.import_module(module_name)

        # Find all classes in the module
        for _, obj in inspect.getmembers(module, inspect.isclass):
            if issubclass(obj, BaseTask) and obj is not BaseTask:  # Ignore base class
                discovered_tasks[obj.__name__] = obj  # Store class reference

    return discovered_tasks

@workflow.defn
class DynamicWorkflow:
    @workflow.run
    async def run(self) -> dict:
        wf_info = workflow.info()
        workflow_id = wf_info.workflow_id
        run_id = wf_info.run_id

        results = {
            "workflow_id": workflow_id,
            "run_id": run_id,
            "task_results": []
        }

        # 🔥 Discover all BaseTask implementations dynamically
        from tasks import base_task  # 👈 Import the package where tasks are defined
        task_classes = discover_Base_tasks(base_task)

        # ✅ Execute each task sequentially
        for task_name, task_class in task_classes.items():
            task_instance = task_class()  # ✅ Instantiate the task dynamically
            step_result = await workflow.execute_activity(
                task_instance.run,  # ✅ Run the entire task
                start_to_close_timeout=timedelta(seconds=60),
            )
            results["task_results"].append({"task_name": task_name, "result": step_result})

        return results


#########
import pkgutil
import importlib
import inspect
from temporalio import workflow
from datetime import timedelta
from result_writer import DBResultWriter, S3ResultWriter  # ✅ Import result writers

def discover_Base_tasks(package):
    """Dynamically discover all BaseTask subclasses."""
    discovered_tasks = {}

    for _, module_name, _ in pkgutil.walk_packages(package.__path__, package.__name__ + "."):
        module = importlib.import_module(module_name)

        for _, obj in inspect.getmembers(module, inspect.isclass):
            if issubclass(obj, BaseTask) and obj is not BaseTask:
                discovered_tasks[obj.__name__] = obj  # Store class reference

    return discovered_tasks

@workflow.defn
class DynamicWorkflow:
    @workflow.run
    async def run(self) -> dict:
        wf_info = workflow.info()
        workflow_id = wf_info.workflow_id
        run_id = wf_info.run_id

        results = {
            "workflow_id": workflow_id,
            "run_id": run_id,
            "task_results": []
        }

        from tasks import base_task
        task_classes = discover_Base_tasks(base_task)

        for task_name, task_class in task_classes.items():
            task_instance = task_class()
            step_result = await workflow.execute_activity(
                task_instance.run,
                start_to_close_timeout=timedelta(seconds=60),
            )
            results["task_results"].append({"task_name": task_name, "result": step_result})

            # ✅ Write results after all tasks complete
            writer_instance = task_instance  # Task already inherits a writer
            write_result = await workflow.execute_activity(
                writer_instance.write,
                step_result,  # ✅ Pass the stored result
                start_to_close_timeout=timedelta(seconds=30),
            )
            results["task_results"].append({"task_name": f"{task_name}_write", "result": write_result})

        return results



###########

import pkgutil
import importlib
import inspect
from temporalio import workflow
from datetime import timedelta
from result_writer import S3ResultWriter, APIResultWriter, DBResultWriter

def discover_Base_tasks(package):
    """Dynamically discover all BaseTask subclasses."""
    discovered_tasks = {}

    for _, module_name, _ in pkgutil.walk_packages(package.__path__, package.__name__ + "."):
        module = importlib.import_module(module_name)

        for _, obj in inspect.getmembers(module, inspect.isclass):
            if issubclass(obj, BaseTask) and obj is not BaseTask:
                discovered_tasks[obj.__name__] = obj  # Store class reference

    return discovered_tasks

@workflow.defn
class DynamicWorkflow:
    @workflow.run
    async def run(self) -> dict:
        wf_info = workflow.info()
        workflow_id = wf_info.workflow_id
        run_id = wf_info.run_id

        results = {
            "workflow_id": workflow_id,
            "run_id": run_id,
            "task_results": []
        }

        from tasks import base_task
        task_classes = discover_Base_tasks(base_task)

        grouped_results = {
            "S3ResultWriter": [],
            "APIResultWriter": [],
            "DBResultWriter": []
        }

        for task_name, task_class in task_classes.items():
            task_instance = task_class()
            task_result = await workflow.execute_activity(
                task_instance.run,
                start_to_close_timeout=timedelta(seconds=60),
            )
            results["task_results"].append({"task_name": task_name, "result": task_result})

            # 🔥 Group results by writer type
            writer_type = task_instance.__class__.__bases__[1].__name__
            grouped_results[writer_type].append(task_result)

        # ✅ Execute writers in order: S3 → API → DB
        for writer_cls in [S3ResultWriter, APIResultWriter, DBResultWriter]:
            writer_name = writer_cls.__name__
            if grouped_results[writer_name]:
                writer_instance = writer_cls()
                write_result = await workflow.execute_activity(
                    writer_instance.write,
                    grouped_results[writer_name],
                    start_to_close_timeout=timedelta(seconds=30),
                )
                results["task_results"].append({"task_name": f"{writer_name}_write", "result": write_result})


        # write_tasks.append(workflow.execute_activity(
        #     writer_instance.write,
        #     grouped_results[writer_name],
        #     start_to_close_timeout=timedelta(seconds=30),
        # ))
        #
        # await asyncio.gather(*write_tasks)  # ✅ Runs all writers in parallel

        return results
