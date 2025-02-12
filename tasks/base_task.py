from abc import ABC, abstractmethod
from temporalio import activity

class BaseTask(ABC):
    """Abstract base class for all Base tasks."""

    @abstractmethod
    async def precheck(self):
        pass

    @abstractmethod
    async def preprocess(self):
        pass

    @abstractmethod
    async def process(self):
        pass

    @abstractmethod
    async def postprocess(self):
        pass

    @abstractmethod
    async def write(self):
        pass

    async def run(self):
        """Runs all the steps in sequence."""
        await self.precheck()
        await self.preprocess()
        await self.process()
        await self.postprocess()
        await self.write()
        return f"{self.__class__.__name__} Completed"





class DataProcessingTask(BaseTask):
    """Data processing implementation of BaseTask."""

    async def precheck(self):
        return "Data Precheck Passed"

    async def preprocess(self):
        return "Data Preprocessing Completed"

    async def process(self):
        return "Data Processing Completed"

    async def postprocess(self):
        return "Data Postprocessing Completed"

    async def write(self):
        return "Data Written to Storage"

    @activity.defn(name="DataProcessingTask")
    async def run(self):
        return await super().run()  # ✅ Calls all steps in sequence



from abc import ABC, abstractmethod

##########Write ######
class BaseTask(ABC):
    """Abstract base class for all Base tasks."""

    def __init__(self):
        self.result = None  # ✅ Store result for later writing

    @abstractmethod
    async def precheck(self):
        pass

    @abstractmethod
    async def preprocess(self):
        pass

    @abstractmethod
    async def process(self):
        pass

    @abstractmethod
    async def postprocess(self):
        pass

    async def run(self):
        """Runs all the steps in sequence and stores the result."""
        await self.precheck()
        await self.preprocess()
        await self.process()
        await self.postprocess()
        self.result = f"{self.__class__.__name__} Completed"  # ✅ Store result
        return self.result



from result_writer import DBResultWriter, S3ResultWriter, APIResultWriter


class DataProcessingTask(BaseTask, S3ResultWriter):
    """Processes data and writes to S3."""

    async def precheck(self):
        return "Data Precheck Passed"

    async def preprocess(self):
        return "Data Preprocessing Completed"

    async def process(self):
        return "Data Processing Completed"

    async def postprocess(self):
        return "Data Postprocessing Completed"

    @activity.defn(name="DataProcessingTask")
    async def run(self):
        return await super().run()  # ✅ Calls all steps in sequence




class APITask(BaseTask, DBResultWriter):
    """Processes API requests and writes to DB."""

    async def precheck(self):
        return "API Precheck Passed"

    async def preprocess(self):
        return "API Preprocessing Completed"

    async def process(self):
        return "API Request Sent"

    async def postprocess(self):
        return "API Response Postprocessing Completed"

    @activity.defn(name="APITask")
    async def run(self):
        return await super().run()  # ✅ Calls all steps in sequence






class DataProcessingTask(BaseTask, S3ResultWriter):
    """Processes data and writes to S3."""

    async def precheck(self): return "Data Precheck Passed"
    async def preprocess(self): return "Data Preprocessing Completed"
    async def process(self): return "Data Processing Completed"
    async def postprocess(self): return "Data Postprocessing Completed"

    @activity.defn(name="DataProcessingTask")
    async def run(self):
        await super().run()
        self.result = {"key": "data-file.json", "data": "Processed data"}
        return self.result

class APITask1(BaseTask, APIResultWriter):
    """Posts API request #1."""

    async def precheck(self): return "API1 Precheck Passed"
    async def preprocess(self): return "API1 Preprocessing Completed"
    async def process(self): return "API1 Request Sent"
    async def postprocess(self): return "API1 Response Processed"

    @activity.defn(name="APITask1")
    async def run(self):
        await super().run()
        self.result = {"endpoint": "https://api.example.com/submit", "data": {"key": "value"}}
        return self.result

class DBTask1(BaseTask, DBResultWriter):
    """Writes to DB (Example 1)."""

    async def precheck(self): return "DBTask1 Precheck Passed"
    async def preprocess(self): return "DBTask1 Preprocessing Completed"
    async def process(self): return "DBTask1 Processing Completed"
    async def postprocess(self): return "DBTask1 Postprocessing Completed"

    @activity.defn(name="DBTask1")
    async def run(self):
        await super().run()
        self.result = {"query": "INSERT INTO results (key, value) VALUES (:key, :value)", "params": {"key": "task1", "value": "result1"}}
        return self.result
