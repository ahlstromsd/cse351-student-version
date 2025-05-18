"""
Course    : CSE 351
Assignment: 04
Student   : Sydney Ahlstrom

Instructions:
    - review instructions in the course

In order to retrieve a weather record from the server, Use the URL:

f'{TOP_API_URL}/record/{name}/{recno}

where:

name: name of the city
recno: record number starting from 0

"""

import time
import threading
import queue
from common import *

from cse351 import *

THREADS = 5
# TODO - set for your program
WORKERS = 10
RECORDS_TO_RETRIEVE = 5000  # Don't change


# ---------------------------------------------------------------------------
def retrieve_weather_data(request_queue, worker_queue, stop_event):
    # TODO - fill out this thread function (and arguments)
    while not stop_event.is_set():
        try:
            # Get the next request from the queue
            city, recno = request_queue.get(timeout=1)
            if city == "DONE":
                worker_queue.put(("DONE", None, None))
                request_queue.put(("DONE", None))
                break
            url = f'{TOP_API_URL}/record/{city}/{recno}'
            data = get_data_from_server(url)
            if data and 'date' in data and 'temp' in data:
                worker_queue.put((city, data['date'], data['temp']))
            request_queue.task_done()
        except queue.Empty:
            continue
        except Exception as e:
            print(f"Error retrieve_weather_data: {e}")
            

# TODO - Create Worker threaded class
class Worker(threading.Thread):
    def __init__(self, worker_queue, noaa, stop_event):
        super().__init__()
        self.worker_queue = worker_queue
        self.noaa = noaa
        self.stop_event = stop_event

    def run(self):
        while not self.stop_event.is_set():
            try:
                city, date, temp = self.worker_queue.get(timeout=1)
                if city == "DONE":
                    self.worker_queue.put(("DONE", None, None))
                    break
                self.noaa.store_data(city, date, temp)
                self.worker_queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                print(f"Error in Worker: {e}")

# ---------------------------------------------------------------------------
# TODO - Complete this class
class NOAA:

    def __init__(self):
        self.data = {city: [] for city in CITIES}
        self.lock = threading.Lock()

    def store_data(self, city, date, temp):
        with self.lock:
            self.data[city].append((date, temp))

    def get_temp_details(self, city):
        with self.lock:
            if not self.data[city]:
                return 0
            total_temp = sum(temp for _, temp in self.data[city])
            return total_temp / len(self.data[city])


# ---------------------------------------------------------------------------
def verify_noaa_results(noaa):

    answers = {
        'sandiego': 14.5004,
        'philadelphia': 14.865,
        'san_antonio': 14.638,
        'san_jose': 14.5756,
        'new_york': 14.6472,
        'houston': 14.591,
        'dallas': 14.835,
        'chicago': 14.6584,
        'los_angeles': 15.2346,
        'phoenix': 12.4404,
    }

    print()
    print('NOAA Results: Verifying Results')
    print('===================================')
    for name in CITIES:
        answer = answers[name]
        avg = noaa.get_temp_details(name)

        if abs(avg - answer) > 0.00001:
            msg = f'FAILED  Excepted {answer}'
        else:
            msg = f'PASSED'
        print(f'{name:>15}: {avg:<10} {msg}')
    print('===================================')


# ---------------------------------------------------------------------------
def main():

    log = Log(show_terminal=True, filename_log='assignment.log')
    log.start_timer()

    noaa = NOAA()

    # Start server
    data = get_data_from_server(f'{TOP_API_URL}/start')
    if data is None:
        print('Server not started')
        log.stop_timer('Run time: ')
        return

    # Get all cities number of records
    print('Retrieving city details')
    city_details = {}
    name = 'City'
    print(f'{name:>15}: Records')
    print('===================================')
    for name in CITIES:
        city_details[name] = get_data_from_server(f'{TOP_API_URL}/city/{name}')
        print(f'{name:>15}: Records = {city_details[name]['records']:,}')
    print('===================================')

    records = RECORDS_TO_RETRIEVE

    # TODO - Create any queues, pipes, locks, barriers you need
    request_queue = queue.Queue(maxsize=10)
    worker_queue = queue.Queue(maxsize=10)
    stop_event = threading.Event()

    retriever_threads = []
    for _ in range(THREADS):
        t = threading.Thread(target=retrieve_weather_data, args=(request_queue, worker_queue, stop_event))
        t.start()
        retriever_threads.append(t)

    worker_threads = []
    for _ in range(WORKERS):
        w = Worker(worker_queue, noaa, stop_event)
        w.start()
        worker_threads.append(w)

    for city in CITIES:
        for recno in range(records):
            request_queue.put((city, recno))

    request_queue.put(("DONE", None))

    request_queue.join()

    stop_event.set()

    for t in retriever_threads:
        t.join()

    worker_queue.join()

    for w in worker_threads:
        w.join()

    data = get_data_from_server(f'{TOP_API_URL}/end')
    if data is None:
        print('Server not started')
    else:
        print(data)

    verify_noaa_results(noaa)

    log.stop_timer('Run time: ')


    # End server - don't change below
    data = get_data_from_server(f'{TOP_API_URL}/end')
    print(data)

    verify_noaa_results(noaa)

    log.stop_timer('Run time: ')


if __name__ == '__main__':
    main()

