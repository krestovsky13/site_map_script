#!/usr/bin/env python3

import time
import re
import requests
import psycopg2
# from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from urllib.parse import urlsplit, urlunsplit
from bs4 import BeautifulSoup
from threading import Thread
from decouple import config


class SiteMap:
    def __init__(self, url, threads=None):
        self.url = url
        self.threads = threads if threads else 5
        self.scheme = urlsplit(url).scheme
        self.netloc = urlsplit(url).netloc
        self.table = self.netloc.replace('-', '_').replace('.', '_')
        self.site_map = [url]
        self.queue = [url]

    @staticmethod
    def _get_page(url):
        try:
            response = requests.get(url, timeout=5)
            if response.ok:
                page = response.text
                return page
            return
        except requests.exceptions.RequestException:
            return

    def _get_internal_links(self, url):
        page = self._get_page(url)
        if page:
            soup = BeautifulSoup(markup=page, features='html.parser')
            for link in soup.find_all('a', href=re.compile(fr"((^{self.url}/.+)|(^/.+))")):
                l_hr = link.get('href')
                rl_hr = self._refactor_link(l_hr)
                print(len(self.site_map))
                if rl_hr not in self.site_map:
                    self.site_map.append(rl_hr)
                    self.queue.append(rl_hr)
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

    def write_file(self):
        with open(f'files/{self.netloc}.txt', 'w', encoding='utf-8') as tx:
            tx.write('\n'.join(self.site_map))
            print(f'Sitemap location: files/{self.netloc}.txt')

    def write_db(self):
        with psycopg2.connect(dbname=config('POSTGRES_DB'), user=config('POSTGRES_USER'), host=config('POSTGRES_HOST'),
                              password=config('POSTGRES_PASSWORD')) as connection:
            # connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            with connection.cursor() as cursor:
                # sql_create_database = '''CREATE DATABASE IF NOT EXISTS avsoft_db'''
                sql_drop_table = f'''DROP TABLE IF EXISTS {self.table};'''
                sql_create_table = f'''CREATE TABLE IF NOT EXISTS {self.table}
                                    (id  INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                                     url VARCHAR(2150));'''

                cursor.execute(sql_drop_table)

                cursor.execute(sql_create_table)

                # args_str = ','.join(cursor.mogrify("(%s)", tuple(x)) for x in self.site_map)
                # cursor.executemany(f'INSERT INTO {self.table} (url) VALUES(%s)', tuple(self.site_map))

                for i in self.site_map:
                    cursor.execute(f'''INSERT INTO {self.table} (url) VALUES(%s)''', tuple({i}))

                connection.commit()


if __name__ == '__main__':
    url = 'http://crawler-test.com'
    threads = 5
    t = time.time()
    sm = SiteMap(url, threads)

    sm.event_loop()
    print(f'Execution time - {time.time() - t}')
    print(f'Len sitemap - {len(sm.site_map)}')
    sm.write_file()
    sm.write_db()
