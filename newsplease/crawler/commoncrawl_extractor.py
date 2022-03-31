#!/usr/bin/env python
"""
Provides functionality to crawl and extract news articles from a single WARC file from commoncrawl.org. Filter criteria, such as publish date
and host list, can be defined. Currently, the WARC file will be downloaded to the path WORKINGDIR/cc_download_warc, if
not otherwise specified.
"""
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
    lev = logging.INFO
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



less_noise()


class CommonCrawlExtractor:
    def __init__(self,
                 save_dir=None,
                 callback_on_warc_completed=None,
                 valid_hosts=None,
                 start_date=None, end_date=None,
                 strict_date=True,
                 reuse_previously_downloaded_files=True,
                 continue_after_error=True, ignore_unicode_errors=False,
                 show_download_progress=False, log_level=logging.INFO, delete_warc_after_extraction=True,
                 logfile=None, fetch_images=False):
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
        self.continue_after_error = continue_after_error
        self.ignore_unicode_errors = ignore_unicode_errors
        self.fetch_images = fetch_images
        self.callback_on_warc_completed = callback_on_warc_completed

        self.show_download_progress = show_download_progress
        self.log_level = log_level
        self.delete_warc_after_extraction = delete_warc_after_extraction
        self.logfile = logfile
        self.download_fs, _ =  url_to_fs(self.save_dir)
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(self.log_level)
        self.example_buffer = {}

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

    def save(self, article):
        """to {save_dir}/article.source_domain/hash(article.filename).json"""
        short_filename = hashlib.sha256(article.filename.encode()).hexdigest()
        domain_dir = os.path.join(self.save_dir, article.source_domain)
        save_path = os.path.join(domain_dir, short_filename + '.json')
        self.download_fs.makedirs(domain_dir, exist_ok=True)
        with self.download_fs.open(save_path, 'w', encoding='utf-8') as outfile:
            json.dump(article.__dict__, outfile, default=str, separators=(',', ':'), ensure_ascii=False, sort_keys=True)

    def process_warc_gz_url(self, url):
        """Iterates all transactions in one WARC file and for each transaction tries to extract an article object.
        Each article is checked against the filter criteria and if all are passed, the article is saved to save_dir
        """
        print(f'Processing {url}')
        self.logger.info(f'URL={url}')
        n_articles = 0
        n_good = 0
        n_bad = 0
        n_error = 0
        start_time = time.time()
        resp = requests.get(url, stream=True)
        for i, record in enumerate(ArchiveIterator(resp.raw)):
            if record.rec_type != 'response':
                continue
            try:
                n_articles += 1
                try:
                    article = self._from_warc(record)
                    article_is_worthy = self.filter_record(record, article)
                except (UnicodeDecodeError, EmptyResponseError):
                    article_is_worthy = False
                    article = None
                if article_is_worthy:
                    n_good += 1
                    self.save(article)
                else:
                    n_bad += 1
                    if article:
                        self.logger.info('article discard (%s; %s; %s)', article.source_domain, article.date_publish, article.title)
                    else:
                        self.logger.info('article discard (%s)', record.rec_headers.get_header('WARC-Target-URI'))

                if n_articles % 5000 == 0:
                    elapsed_secs = time.time() - start_time
                    secs_per_article = elapsed_secs / n_articles
                    self.logger.info('pass = %i, discard = %i, error = %i, total = %i', n_good, n_bad, n_error, n_articles)
                    self.logger.info(f'extraction from current WARC {url} started {human(start_time)}; %f s/article/process', human(start_time), secs_per_article)
            except:
                if False: #self.continue_after_error:
                    self.logger.error('Unexpected error: %s (%s)', *sys.exc_info()[0:2])
                    self.logger.error(sys.exc_info()[2], exc_info=True)
                    n_error += 1
                    pass
                else:
                    raise
        self.logger.info(f'Completed WARC {url}')
        self.save_status(url, 1 if n_good > 0 else 0)
        self.callback_on_warc_completed(url, n_good, n_bad, n_error, n_articles)
        return 1

    def save_status(self, url, status):
        print(f' {url} status={status}')
        msg = f'{url},{status}'

        with FileLock(self.logfile):
            with open(self.logfile, 'a') as log_file:
                log_file.writelines([msg])
                log_file.flush()


def path_exists(uri_or_path):
    fs, _ = url_to_fs(uri_or_path)
    return fs.exists(uri_or_path)