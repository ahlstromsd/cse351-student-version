"""
Course: CSE 351 
Lesson: L03 team activity
File:   team.py
Author: <Add name here>

Purpose: Retrieve Star Wars details from a server

Instructions:

- This program requires that the server.py program be started in a terminal window.
- The program will retrieve the names of:
    - characters
    - planets
    - starships
    - vehicles
    - species

- the server will delay the request by 0.5 seconds

TODO
- Create a threaded function to make a call to the server where
  it retrieves data based on a URL.  The function should have a method
  called get_name() that returns the name of the character, planet, etc...
- The threaded function should only retrieve one URL.
- Create a queue that will be used between the main thread and the threaded functions

- Speed up this program as fast as you can by:
    - creating as many as you can
    - start them all
    - join them all

"""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import threading
from common import *
from queue import Queue

# Include cse 351 common Python files
from cse351 import *

# global
call_count = 0

class ReadURL(threading.Thread):
    def __init__(self, url):
        super().__init__()
        self.url = url
        self.name = ""
    
    def get_name(self):
        return self.name

    def run(self):

        item = get_data_from_server(self.url)
        self.name = item['name']

def readurl(url):
    
    item = get_data_from_server(url)
    return item['name']

def get_urls(film6, kind, q):
    global call_count

    urls = film6[kind]
    print(kind)
    for url in urls:
        call_count += 1
        # item = get_data_from_server(url)
        # print(f'  - {item['name']}')
        q.put(url)

def main():
    global call_count

    log = Log(show_terminal=True)
    log.start_timer('Starting to retrieve data from the server')

    film6 = get_data_from_server(f'{TOP_API_URL}/films/6')
    call_count += 1
    print_dict(film6)

    q = Queue()
    threads = []

    # Retrieve people
    get_urls(film6, 'characters',q)
    get_urls(film6, 'planets',q)
    get_urls(film6, 'starships',q)
    get_urls(film6, 'vehicles',q)
    get_urls(film6, 'species',q)
    q.put(None)

    executor = ThreadPoolExecutor(max_workers=10)
    with executor as ex:
        while True:
            url = q.get()
            if url is None:
                break
            executor.map(readurl, url)

    # while True:
    #     if q.get() is None:
    #         break
    #     t = ReadURL(q.get())
    #     threads.append(t)
    #     t.start()
    
    # for t in threads:
    #     t.join()
    #     print(f'  - {t.get_name()}')

    log.stop_timer('Total Time To complete')
    log.write(f'There were {call_count} calls to the server')

if __name__ == "__main__":
    main()
