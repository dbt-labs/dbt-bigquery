import google.cloud.bigquery
import queue

from threading import Thread
from typing import Dict

from dbt.events import AdapterLogger
DATASET_UPDATE_QUEUE: queue.SimpleQueue = queue.SimpleQueue()
__DATASET_UPDATE_THREAD: Dict[str, Thread] = {}

logger = AdapterLogger("BigQuery")

def update_dataset(client: google.cloud.bigquery.Client, update):
        start_dataset_update_thread(client)
        DATASET_UPDATE_QUEUE.put(update)


def start_dataset_update_thread(client: google.cloud.bigquery.Client) -> None:
    if "thread" in __DATASET_UPDATE_THREAD:
        dataset_update_thread: Thread = __DATASET_UPDATE_THREAD["thread"]
    dataset_update_thread = DatasetAsyncUpdater(client=client)
    dataset_update_thread.start()
    __DATASET_UPDATE_THREAD["thread"] = dataset_update_thread


class DatasetAsyncUpdater(Thread):
    client: google.cloud.bigquery.Client

    def __init__(self, client) -> None:
        super().__init__()
        self.daemon = True # will close when the parent process closes
        self.client = client

    def run(self) -> None:
        while True:
            val = DATASET_UPDATE_QUEUE.get()
            if val:
                self.client.update_dataset(**val)