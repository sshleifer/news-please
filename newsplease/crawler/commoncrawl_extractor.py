#!/usr/bin/env python
"""
Provides functionality to crawl and extract news articles from a single WARC file from commoncrawl.org. Filter criteria, such as publish date
and host list, can be defined. Currently, the WARC file will be downloaded to the path WORKINGDIR/cc_download_warc, if
not otherwise specified.
"""
import logging
import os
import subprocess
import sys
import time
import hashlib
from ago import human
from dateutil import parser
from hurry.filesize import size
from scrapy.utils.log import configure_logging
from six.moves import urllib
from warcio.archiveiterator import ArchiveIterator
from fsspec.core import url_to_fs
import json
from .. import NewsPlease, EmptyResponseError

logging.getLogger('requests').setLevel(logging.CRITICAL)
logging.getLogger('readability').setLevel(logging.CRITICAL)
logging.getLogger("botocore").setLevel(logging.CRITICAL)  # Much noise
logging.getLogger("botocore.credentials").setLevel(logging.CRITICAL)  # Much noise
logging.getLogger("aibotocore.credentials").setLevel(logging.CRITICAL)  # Much noise
logging.getLogger('PIL').setLevel(logging.CRITICAL)
logging.getLogger('newspaper').setLevel(logging.CRITICAL)
logging.getLogger('newsplease').setLevel(logging.CRITICAL)
logging.getLogger('urllib3').setLevel(logging.CRITICAL)


class CommonCrawlExtractor:
    def __init__(self,
                 save_dir=None,
                 callback_on_warc_completed=None,
                 valid_hosts=None,
                 start_date=None, end_date=None,
                 strict_date=True, reuse_previously_downloaded_files=True,
                 continue_after_error=True, ignore_unicode_errors=False,
                 show_download_progress=False, log_level=logging.ERROR, delete_warc_after_extraction=True,
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
        self.reuse_previously_downloaded_files = reuse_previously_downloaded_files
        self.continue_after_error = continue_after_error
        self.ignore_unicode_errors = ignore_unicode_errors
        self.fetch_images = fetch_images
        self.callback_on_warc_completed = callback_on_warc_completed

        self.show_download_progress = show_download_progress
        self.log_level = log_level
        self.delete_warc_after_extraction = delete_warc_after_extraction
        self.logfile = logfile
        #os.makedirs(self.save_dir, exist_ok=True)
        self.download_fs, _ =  url_to_fs(self.save_dir)
        self._set_loggers()


    def _set_loggers(self):
        # try to make loggers quiet
        configure_logging({"LOG_LEVEL": "ERROR"})
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(self.log_level)

    def filter_record(self, warc_record, article=None):
        """
        Returns true if a record passes all tests: hosts, publishing date
        :param warc_record:
        :return: A tuple of (True or False) and an article (might be None)
        """
        # filter by host
        if self.filter_valid_hosts:
            url = warc_record.rec_headers.get_header('WARC-Target-URI')

            # very simple check, check if one of the required host names is contained in the url of the WARC transaction
            # better would be to extract the host name from the WARC transaction Target URI and then check for equality
            # because currently something like g.co?forward_url=facebook.com would yield a positive filter test for
            # facebook.com even though the actual host is g.co
            for valid_host in self.filter_valid_hosts:
                if valid_host in url:
                    break
            else:
                return False, article

        # filter by date
        if self.start_date or self.end_date:
            if not article:
                article = self._from_warc(warc_record)

            publishing_date = self.__get_publishing_date(warc_record, article)
            if not publishing_date:
                if self.strict_date:
                    return False, article
            else:  # here we for sure have a date
                # is article published too early?
                if self.start_date and publishing_date < self.start_date:
                    return False, article
                if self.end_date and publishing_date > self.end_date:
                    return False, article

        return True, article

    def __get_publishing_date(self, warc_record, article):
        """
        Extracts the publishing date from the record
        :param warc_record:
        :return:
        """
        if hasattr(article, 'date_publish'):
            return parser.parse(article.date_publish) if isinstance(article.date_publish, str) else article.date_publish
        else:
            return None

    def __on_download_progress_update(self, blocknum, blocksize, totalsize):
        """
        Prints some download progress information
        :param blocknum:
        :param blocksize:
        :param totalsize:
        :return:
        """
        if not self.show_download_progress:
            return

        readsofar = blocknum * blocksize
        if totalsize > 0:
            s = "\r%s / %s" % (size(readsofar), size(totalsize))
            sys.stdout.write(s)
            if readsofar >= totalsize:  # near the end
                sys.stderr.write("\r")
        else:  # total size is unknown
            sys.stdout.write("\rread %s" % (size(readsofar)))

    def _from_warc(self, record):
        return NewsPlease.from_warc(record, decode_errors="replace" if self.ignore_unicode_errors else "strict", fetch_images=self.fetch_images)

    def save(self, article):
        """to {save_dir}/article.source_domain/hash(article.filename).json"""
        short_filename = hashlib.sha256(article.filename.encode()).hexdigest()
        domain_dir = os.path.join(self.save_dir, article.source_domain)
        save_path = os.path.join(domain_dir, short_filename + '.json')
        self.download_fs.makedirs(domain_dir, exist_ok=True)
        with self.download_fs.open(save_path, 'w', encoding='utf-8') as outfile:
            json.dump(article.__dict__, outfile, default=str, separators=(',', ':'), ensure_ascii=False, sort_keys=True)

    def process_warc_gz_file(self, url):
        """
        Iterates all transactions in one WARC file and for each transaction tries to extract an article object.
        Afterwards, each article is checked against the filter criteria and if all are passed, the function
        on_valid_article_extracted is invoked with the article object.
        """

        print(f'Processing {url}')
        counter_article_total = 0
        counter_article_passed = 0
        counter_article_discarded = 0
        counter_article_error = 0
        start_time = time.time()

        #with self.download_fs.open(path_name, 'rb') as stream:
        import requests, warcio
        resp = requests.get(url, stream=True)
        try:
            archive_iterator = ArchiveIterator(resp.raw)
        except warcio.exceptions.ArchiveLoadFailed:
            self.save_status(url, 0)
            return 0

        for record in archive_iterator:
            try:
                if record.rec_type == 'response':
                    counter_article_total += 1

                    # if the article passes filter tests, we notify the user
                    try:
                        article_is_worthy, article = self.filter_record(record)
                    except (UnicodeDecodeError, EmptyResponseError):
                        article_is_worthy = False
                    if article_is_worthy:
                        try:
                            if not article:
                                article = self._from_warc(record)
                        except (UnicodeDecodeError, EmptyResponseError):
                            article_is_worthy = False
                    if article_is_worthy:
                        counter_article_passed += 1

                        #self.logger.info('article pass (%s; %s; %s)', article.source_domain, article.date_publish, article.title)
                        self.save(article)
                    else:
                        counter_article_discarded += 1

                        if article:
                            self.logger.info('article discard (%s; %s; %s)', article.source_domain,
                                             article.date_publish,
                                             article.title)
                        else:
                            self.logger.info('article discard (%s)',
                                             record.rec_headers.get_header('WARC-Target-URI'))

                    if counter_article_total % 100 == 0:
                        elapsed_secs = time.time() - start_time
                        secs_per_article = elapsed_secs / counter_article_total
                        self.logger.info('statistics')
                        self.logger.info('pass = %i, discard = %i, error = %i, total = %i',
                                         counter_article_passed,
                                         counter_article_discarded, counter_article_error, counter_article_total)
                        self.logger.info('extraction from current WARC file started %s; %f s/article/process',
                                         human(start_time), secs_per_article)
            except:
                self.save_status(url)
                if False: #self.continue_after_error:
                    self.logger.error('Unexpected error: %s (%s)', *sys.exc_info()[0:2])
                    self.logger.error(sys.exc_info()[2], exc_info=True)
                    counter_article_error += 1
                    pass
                else:
                    raise
        self.save_status(url, 1)
        self.callback_on_warc_completed(url, counter_article_passed, counter_article_discarded,
                                          counter_article_error, counter_article_total)

        return 1


    def save_status(self, url, status):
        print(f' {url} status={status}')
        from  filelock import FileLock
        msg = f'{url},{status}'

        with FileLock(self.logfile):
            with open(self.logfile, 'a') as log_file:
                log_file.writelines([msg])
                log_file.flush()
        return

        pass
        # if status:
        #     with open(self.logfile, 'a') as log_file:
        #         log_file.write(f'{} {url}'



def path_exists(uri_or_path):
    fs, _ = url_to_fs(uri_or_path)
    return fs.exists(uri_or_path)