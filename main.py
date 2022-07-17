#!/usr/bin/env python3

import logging
import queue
import threading
import concurrent.futures
import random
import time
import urllib.request
from urllib.parse import urlparse, urlsplit, urlunsplit, quote
import requests
from bs4 import BeautifulSoup
import re

#
# def producer(queue: queue.Queue, event: threading.Event):
#     """Получаем число из сети."""
#     while not event.is_set():
#         message = random.randint(1, 101)
#         logging.info("Producer got message: %s", message)
#         queue.put(message)
#     logging.info("Producer received event. Exiting")
#
#
# def consumer(queue: queue.Queue, event: threading.Event):
#     """Сохраняем число в БД"""
#     while not event.is_set() or not pipeline.empty():
#         message = queue.get()
#         logging.info(
#             "Consumer storing message: %s (size=%d)", message, queue.qsize()
#         )
#     logging.info("Consumer received event. Exiting")
#
#
# if __name__ == "__main__":
#     format = "%(asctime)s: %(message)s"
#     logging.basicConfig(format=format, level=logging.INFO,
#                         datefmt="%H:%M:%S")
#     # logging.getLogger().setLevel(logging.DEBUG)
#
#     event = threading.Event()
#     pipeline = queue.Queue(maxsize=10)
#     with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
#         executor.submit(consumer, pipeline, event)
#         executor.submit(producer, pipeline, event)
#
#         time.sleep(1)
#         event.set()

# import queue
from threading import Thread

#
# visited = set()
queue = queue.Queue()


class SiteMap:
    def __init__(self, url, threads=None):
        self.url = url
        self.threads = threads if threads else 1
        self.scheme = urlsplit(url).scheme
        self.netloc = urlsplit(url).netloc
        self.site_map = [url]
        self.queue = [url]

    def _get_page(self, url):
        try:
            response = requests.get(url, timeout=5, allow_redirects=False)
            if response.ok:
                page = response.text
                return page
        except requests.exceptions.RequestException:
            return

    def _get_internal_links(self, url):
        page = self._get_page(url)
        if page:
            soup = BeautifulSoup(page, features='lxml')
            for link in soup.find_all('a', href=re.compile(fr"((^{self.url}/.+)|(^/.+))")):
                l_hr = link.get('href')
                rl_hr = self._refactor_link(l_hr)
                if rl_hr not in self.site_map:
                    self.site_map.append(rl_hr)
                    self.queue.append(rl_hr)
                    print(len(self.site_map), self.site_map)
        else:
            return

    def _refactor_link(self, link):
        if link.startswith('/'):
            link = urlunsplit(components=[self.scheme, self.netloc, link, '', ''])
        if link.endswith('/'):
            link = link[:-1]
        return link

    def event_loop(self):
        while self.queue:
            executors = []
            for i in range(self.threads):
                if not self.queue:
                    continue
                executor = Thread(target=self._get_internal_links, args=(self.queue.pop(0),))
                executor.start()
                executors.append(executor)
            for executor in executors:
                executor.join()


if __name__ == '__main__':
    t = time.time()
    a = SiteMap('http://crawler-test.com', 2)

    a.event_loop()
    # print(a.site_map)
    # print(len(a.site_map))
    print(time.time() - t)
# soup = BeautifulSoup(requests.get('https://stackoverflow.com').text, features='html.parser')
# for link in soup.find_all('a', href=re.compile(fr"((^'https://stackoverflow.com'/.+)|(^/.+))")):
#     l_hr = link.get('href')
#     print(l_hr)
