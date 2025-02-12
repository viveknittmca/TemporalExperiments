from abc import ABC, abstractmethod
from temporalio import activity


class ResultWriter(ABC):
    """Abstract mixin for writing results to different destinations."""

    @abstractmethod
    async def write(self, data):
        pass


class S3ResultWriter(ResultWriter):
    """Writes results to S3."""

    @activity.defn(name="S3ResultWriter")
    async def write(self, data):
        print(f"Writing to S3: {data}")
        return "S3 Write Successful"


class DBResultWriter(ResultWriter):
    """Writes results to a database."""

    @activity.defn(name="DBResultWriter")
    async def write(self, data):
        print(f"Writing to DB: {data}")
        return "DB Write Successful"


class APIResultWriter(ResultWriter):
    """Writes results to an API."""

    @activity.defn(name="APIResultWriter")
    async def write(self, data):
        print(f"Sending to API: {data}")
        return "API Write Successful"


import boto3
import requests
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, text
from temporalio import activity

# 🔹 Define DB engine & session
DATABASE_URL = "postgresql://user:password@db_host/db_name"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

class S3ResultWriter(ResultWriter):
    """Writes results to S3 in a batch."""
    s3_client = boto3.client("s3")

    @activity.defn(name="S3ResultWriter")
    async def write(self, results):
        for result in results:
            self.s3_client.put_object(Bucket="my-bucket", Key=result["key"], Body=result["data"])
        return "S3 Write Successful"

class APIResultWriter(ResultWriter):
    """Sends results to an API in a batch."""

    @activity.defn(name="APIResultWriter")
    async def write(self, results):
        for result in results:
            response = requests.post(result["endpoint"], json=result["data"])
            response.raise_for_status()
        return "API Write Successful"

class DBResultWriter(ResultWriter):
    """Writes results to a database in a single transaction."""

    @activity.defn(name="DBResultWriter")
    async def write(self, results):
        session = SessionLocal()
        try:
            for result in results:
                session.execute(text(result["query"]), result["params"])
            session.commit()  # ✅ Ensure atomic transaction
            return "DB Write Successful"
        except Exception as e:
            session.rollback()  # ❌ Rollback on failure
            raise e
        finally:
            session.close()


from temporalio.common import RetryPolicy
import requests
from temporalio import activity

class APIResultWriter(ResultWriter):
    """Sends results to an API in a batch."""

    @activity.defn(name="APIResultWriter")
    async def write(self, results):
        retry_policy = RetryPolicy(
            initial_interval=5,  # Wait 5 seconds before first retry
            backoff_coefficient=2,  # Exponential backoff
            maximum_interval=60,  # Max wait time of 60 seconds
            maximum_attempts=5  # Fail after 5 retries
        )

        for result in results:
            for attempt in range(retry_policy.maximum_attempts):
                try:
                    response = requests.post(result["endpoint"], json=result["data"])
                    response.raise_for_status()
                    break  # ✅ Success, move to next result
                except requests.exceptions.RequestException as e:
                    if attempt == retry_policy.maximum_attempts - 1:
                        raise e  # ❌ Give up after 5 failed attempts
                    await asyncio.sleep(min(retry_policy.initial_interval * (2 ** attempt), retry_policy.maximum_interval))
        return "API Write Successful"
