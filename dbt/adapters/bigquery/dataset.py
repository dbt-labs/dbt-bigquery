import threading
import google.cloud.bigquery
import queue

from threading import Thread
from typing import Dict

from dbt.events import AdapterLogger

_dataset_update_queue: queue.SimpleQueue = queue.SimpleQueue()
_dataset_update_thread_store: Dict[str, Thread] = {}
_dataset_update_lock: threading.Lock = threading.Lock()

logger: AdapterLogger = AdapterLogger("BigQuery")


def update_dataset(client: google.cloud.bigquery.Client, update):
    _start_dataset_update_thread(client)
    _dataset_update_queue.put(update)


def _start_dataset_update_thread(client: google.cloud.bigquery.Client) -> None:
    with _dataset_update_lock:
        if "thread" not in _dataset_update_thread_store:
            dataset_update_thread = DatasetAsyncUpdater(client=client, queue=_dataset_update_queue)
            dataset_update_thread.start()
            _dataset_update_thread_store["thread"] = dataset_update_thread


class DatasetAsyncUpdater(Thread):
    client: google.cloud.bigquery.Client
    _queue: queue.SimpleQueue
    _completed_tasks: set

    def __init__(self, client, queue) -> None:
        super().__init__()
        self.daemon = True  # will close when the parent process closes
        self.client = client
        self._queue = queue
        self._completed_tasks = set()

    def run(self) -> None:
        while True:
            val = self._queue.get()
            if val and str(val) not in self._completed_tasks:
                try:
                    self.client.update_dataset(*val)
                    self._completed_tasks.add(str(val))
                except Exception as e:
                    logger.error(e)
