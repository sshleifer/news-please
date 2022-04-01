#!/usr/bin/env python
"""
Provides functionality to crawl and extract news articles from a single WARC file from commoncrawl.org. Filter criteria, such as publish date
and host list, can be defined. Currently, the WARC file will be downloaded to the path_or_url WORKINGDIR/cc_download_warc, if
not otherwise specified.
"""
import datetime
import logging
import os

import sys
from filelock import FileLock
import time
import hashlib
from ago import human
from dateutil import parser
from warcio.archiveiterator import ArchiveIterator
import requests
from fsspec.core import url_to_fs
import json
from .. import NewsPlease, EmptyResponseError


def less_noise():
    lev = logging.ERROR
    logging.getLogger('requests').setLevel(lev)
    logging.getLogger('readability').setLevel(lev)
    logging.getLogger("botocore").setLevel(lev)  # Much noise
    logging.getLogger("botocore.credentials").setLevel(lev)  # Much noise
    logging.getLogger("aibotocore.credentials").setLevel(lev)  # Much noise
    logging.getLogger('PIL').setLevel(lev)
    logging.getLogger('newspaper').setLevel(lev)
    logging.getLogger('newsplease').setLevel(lev)
    logging.getLogger('urllib3').setLevel(lev)
    logging.getLogger('jieba').setLevel(lev)
    logging.getLogger('s3fs').setLevel(lev)
    logging.getLogger('filelock').setLevel(lev)
    logging.getLogger('jieba').setLevel(lev)

def buffer_to_files():
    pass

less_noise()
from typing import List, Tuple

class CommonCrawlExtractor:
    def __init__(self,
                 save_dir, callback_on_warc_completed=None, valid_hosts=None, start_date=None, end_date=None,
                 strict_date=True, ignore_unicode_errors=False, log_level=logging.INFO, fetch_images=False
                 ):
        """
        Crawl and extract articles form the news crawl provided by commoncrawl.org. For each article that was extracted
        successfully the callback function callback_on_article_extracted is invoked where the first parameter is the
        article object.
        """
        self.filter_valid_hosts = valid_hosts
        self.start_date = start_date
        self.end_date = end_date
        self.strict_date = strict_date
        self.save_dir = save_dir

        self.ignore_unicode_errors = ignore_unicode_errors
        self.fetch_images = fetch_images
        self.callback_on_warc_completed = callback_on_warc_completed
        self.log_level = log_level
        self.download_fs, _ =  url_to_fs(self.save_dir) # could del
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(self.log_level)
        self.example_buffer: List[NewsPlease] = []
        self.n_files_saved = 0

    def filter_record(self, warc_record, article) -> bool:
        """Returns true if a record passes all tests: hosts, publishing date"""
        if self.filter_valid_hosts:
            url = warc_record.rec_headers.get_header('WARC-Target-URI')
            # very simple check, check if one of the required host names is contained in the url of the WARC transaction
            # better would be to extract the host name from the WARC transaction Target URI and then check for equality
            # because currently something like g.co?forward_url=facebook.com would yield a positive filter test for
            # facebook.com even though the actual host is g.co
            for valid_host in self.filter_valid_hosts:
                if valid_host in url:
                    return False

        if self.start_date or self.end_date:
            pdate = getattr(article, 'date_publish', None)
            publishing_date = parser.parse(pdate) if isinstance(pdate, str) else pdate
            if not publishing_date:
                if self.strict_date:
                    return False
            else:  # here we for sure have a date
                # is article published too early?
                if self.start_date and publishing_date < self.start_date:
                    return False
                if self.end_date and publishing_date > self.end_date:
                    return False
        return True

    def _from_warc(self, record) -> NewsPlease:
        decode_errors = "replace" if self.ignore_unicode_errors else "strict"
        return NewsPlease.from_warc(record, decode_errors=decode_errors, fetch_images=self.fetch_images)

    def save(self):
        """to {save_dir}/article.source_domain/hash(article.filename).json"""

        short_filename = hashlib.sha256(article.filename.encode()).hexdigest()
        domain_dir = os.path.join(self.save_dir, article.source_domain)
        save_path = os.path.join(domain_dir, short_filename + '.json')
        self.download_fs.makedirs(domain_dir, exist_ok=True)
        with self.download_fs.open(save_path, 'w', encoding='utf-8') as outfile:
            json.dump(article.__dict__, outfile, default=str, separators=(',', ':'), ensure_ascii=False, sort_keys=True)

    def save_and_reset_buffer(self, subdir, i):
        save_path = os.path.join(subdir, f'last_record_{i}.json')
        data = [x.__dict__ for x in self.example_buffer]
        if not data: return
        with self.download_fs.open(save_path, 'w', encoding='utf-8') as outfile:
            json.dump(data, outfile, default=str, separators=(',', ':'), ensure_ascii=False, sort_keys=True)
        self.example_buffer = []
        self.n_files_saved += 1

    def process_warc_gz_url(self, url, subdir, flush_frequency=10000, stop_early=False, resume_idx=0):
        """Iterates all transactions in one WARC file and for each transaction tries to extract an article object.
        Each article is checked against the filter criteria and if all are passed, the article is saved to save_dir
        """
        self.logger.info(f'URL={url}, SAVE_DIR={subdir}')
        print(f'URL={url}, SAVE_DIR={subdir}')

        # Assume the filename pattern is CC-NEWS-20160911145202-00018.warc.gz

        n_good = 0
        start_time = time.time()
        resp = requests.get(url, stream=True)
        def log_msg(i):
            elapsed_secs = time.time() - start_time
            n_processed = (i - resume_idx)
            secs_per_article = elapsed_secs / n_processed
            frac_bad = 1 - n_good/n_processed
            process_throughput = i/elapsed_secs
            msg = f'{url}: {i} {elapsed_secs // 60} mins, n={n_processed}, bad={frac_bad:.2f}, rate={process_throughput:.2f}'
            return msg

        for i, record in enumerate(ArchiveIterator(resp.raw)):
            if i < resume_idx: continue
            if record.rec_type != 'response': continue
            try:
                article = self._from_warc(record)
            except (UnicodeDecodeError, EmptyResponseError):
                continue
            n_good += 1
            self.example_buffer.append(article)
            if len(self.example_buffer) >= flush_frequency:
                self.save_and_reset_buffer(subdir, i)
                self.logger.info(log_msg(i))
                if stop_early: break


        self.save_and_reset_buffer(subdir, i)
        msg = f'Completed: {log_msg(i)}'
        self.logger.info(msg)
        self.callback_on_warc_completed(url, n_good, i-n_good, 0, i)
        return self.n_files_saved


def path_exists(uri_or_path):
    fs, _ = url_to_fs(uri_or_path)
    return fs.exists(uri_or_path)


def fname_to_date(path_or_url):
    fn = os.path.basename(path_or_url)
    # Assume the filename pattern is CC-NEWS-20160911145202-00018.warc.gz
    fn = fn.replace('CC-NEWS-', '')
    dt = fn.split('-')[0]
    try:
        return datetime.datetime.strptime(dt, '%Y%m%d%H%M%S').date()
    except ValueError as e:
        #raise ValueError(f'Could not convert fn={fn},dt={dt}')
        return None #20210327090019