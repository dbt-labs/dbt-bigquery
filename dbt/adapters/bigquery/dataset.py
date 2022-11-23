import threading
import google.cloud.bigquery
import queue

from threading import Thread
from typing import Dict

from dbt.events import AdapterLogger

__DATASET_UPDATE_QUEUE = queue.SimpleQueue()
__DATASET_UPDATE_THREAD: Dict[str, Thread] = {}
__DATASET_UPDATE_LOCK = threading.Lock()

logger: AdapterLogger = AdapterLogger("BigQuery")


def update_dataset(client: google.cloud.bigquery.Client, update):
    _start_dataset_update_thread(client)
    __DATASET_UPDATE_QUEUE.put(update)


def _start_dataset_update_thread(client: google.cloud.bigquery.Client) -> None:
    with __DATASET_UPDATE_LOCK:
        if "thread" not in __DATASET_UPDATE_THREAD:
            dataset_update_thread = DatasetAsyncUpdater(client=client, queue=__DATASET_UPDATE_QUEUE)
            dataset_update_thread.start()
            __DATASET_UPDATE_THREAD["thread"] = dataset_update_thread


class DatasetAsyncUpdater(Thread):
    client: google.cloud.bigquery.Client
    _queue: queue.SimpleQueue

    def __init__(self, client, queue) -> None:
        super().__init__()
        self.daemon = True  # will close when the parent process closes
        self.client = client
        self._queue = queue

    def run(self) -> None:
        while True:
            val = self._queue.get()
            if val:
                try:
                    self.client.update_dataset(*val)
                except Exception as e:
                    logger.error(e)
