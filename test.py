from gcs_mutex_lock import gcs_lock
import logging
import sys
import multiprocessing as mp
import os
from google.cloud import storage
import time

"""
Multiple processes try to acquire the same lock.
Check if only the process which acquired the lock can enter the critical section.
"""

bucket_name="BUCKET_NAME"
lock_file="lock"
N=300
PROCS=13

storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)

def func(counter, lock):
    while True:
        if counter.value >= N:
            break

        locked = gcs_lock.wait_for_lock_expo(bucket_name, lock_file, content=str(os.getpid()))
        if not locked:
            time.sleep(1)
            continue
        with lock:
            counter.value += 1

        print(f"{counter.value} / {N}")
        
        blob = bucket.blob(lock_file)
        s = blob.download_as_string()

        if int(s) == os.getpid():
            pass
        else:
            print(f"!!!!pid: {os.getpid()}, content: {s}")
        gcs_lock.unlock(bucket_name, lock_file)

if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    logging.info("Test start")

    counter = mp.Value('i', 0)
    lock = mp.Lock()

    processes = [mp.Process(target=func, args=(counter, lock)) for _ in range(PROCS)]

    for process in processes:
            process.start()

    for process in processes:
        process.join()