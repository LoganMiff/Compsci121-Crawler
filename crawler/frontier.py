import os
import shelve
import dbm.dumb

from urllib.parse import urlparse
from threading import Thread, RLock
from queue import Queue, Empty
from collections import defaultdict
import time

from utils import get_logger, get_urlhash, normalize
from scraper import is_valid

class Frontier(object):
    def __init__(self, config, restart):
        self.logger = get_logger("FRONTIER")
        self.config = config
        
        # Multithreading
        self.subdomain_queues = defaultdict(lambda: Queue)
        self.in_progress_domains = set()
        self.domainLastAccessed = {}

        self.lock = RLock()
        
        if not os.path.exists(self.config.save_file) and not restart:
            # Save file does not exist, but request to load save.
            self.logger.info(
                f"Did not find save file {self.config.save_file}, "
                f"starting from seed.")
        elif os.path.exists(self.config.save_file) and restart:
            # Save file does exists, but request to start from seed.
            self.logger.info(
                f"Found save file {self.config.save_file}, deleting it.")
            os.remove(self.config.save_file)
            for ext in ['.bak', '.dat', '.dir']:
                if os.path.exists(self.config.save_file + ext):
                    os.remove(self.config.save_file + ext)
        # Load existing save file, or create one if it does not exist.
        self.save = shelve.Shelf(dbm.dumb.open(self.config.save_file, 'c'))
        if restart:
            for url in self.config.seed_urls:
                self.add_url(url)
        else:
            # Set the frontier state with contents of save file.
            self._parse_save_file()
            if not self.save:
                for url in self.config.seed_urls:
                    self.add_url(url)

    
    def _parse_save_file(self):
        ''' This function can be overridden for alternate saving techniques. '''
        with self.lock:
            total_count = len(self.save)
            tbd_count = 0
            for url, completed in self.save.values():
                if not completed and is_valid(url):
                    # Organize by the domain
                    domain = urlparse(url).netloc
                    self.subdomain_queues[domain].put(url)
                    tbd_count += 1
            self.logger.info(
                f"Found {tbd_count} urls to be downloaded from {total_count} "
                f"total urls discovered.")
        

    def get_tbd_url(self):
        with self.lock:
            current_time = time.time()
            empty_queues = []
            domain_to_add = None

            # Find the domain
            for domain, q in self.subdomain_queues.items():
                if q.empty():
                    empty_queues.append(domain)
                    continue

                if domain in self.in_progress_domains:
                    continue
                
                last_time = self.domainLastAccessed.get(domain, -100)
                time_diff = current_time - last_time
                
                if time_diff >= self.config.time_delay:
                    domain_to_add = domain
                    break
                
            for empty_queue in empty_queues:
                del self.subdomain_queues[empty_queue]

            if not domain_to_add:
                return None
            
            self.in_progress_domains.add(domain_to_add)
            self.domainLastAccessed[domain] = time.time() + 0.1

            url = self.subdomain_queues[domain_to_add].get_nowait()
            self.subdomain_queues[domain_to_add].task_done()

            return url
        

    def add_url(self, url):
        url = normalize(url)
        urlhash = get_urlhash(url)
        domain = ".".join(urlparse(url).netloc.split(".")[-3:])

        with self.lock:
            if urlhash not in self.save:
                self.save[urlhash] = (url, False)
                self.save.sync()

                self.subdomain_queues[domain].put(url)
    

    def mark_url_complete(self, url):
        urlhash = get_urlhash(url)

        with self.lock:
            if urlhash not in self.save:
                # This should not happen.
                self.logger.error(
                    f"Completed url {url}, but have not seen it before.")
    
            self.save[urlhash] = (url, True)
            self.save.sync()
            
            # Remove the domain
            domain = ".".join(urlparse(url).netloc.split(".")[-3:])
            if domain in self.in_progress_domains:
                self.in_progress_domains.remove(domain)
