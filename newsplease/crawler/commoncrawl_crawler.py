#!/usr/bin/env python
"""
Provides functionality to crawl and extract news articles from commoncrawl.org. Filter criteria, such as publish date
and host list, can be defined. Currently, all WARC files will be downloaded to the path WORKINGDIR/cc_download_warc, if
not otherwise specified.
"""
import logging
from multiprocessing.sharedctypes import Value
import os
import subprocess
import tempfile
import time
from functools import partial
from multiprocessing import Pool
import datetime
import socket
from dateutil import parser
from scrapy.utils.log import configure_logging

from ..crawler.commoncrawl_extractor import CommonCrawlExtractor

from fire import Fire
import hashlib
import json
import logging
import os
import sys
import datetime
from datetime import date

import socket
import sh


logger = logging.getLogger(__name__)
log_level = logging.INFO
logging.basicConfig(level=log_level)

SLURMLIST="inst-0kygf-elegant-pegasus,inst-1peuj-elegant-pegasus,inst-4ilmp-elegant-pegasus,inst-4mhhw-elegant-pegasus,inst-6utfo-elegant-pegasus,inst-8cas1-elegant-pegasus,inst-bm1tl-elegant-pegasus,inst-c3vih-elegant-pegasus,inst-ceia2-elegant-pegasus,inst-cu41e-elegant-pegasus,inst-czlp2-elegant-pegasus,inst-dxwdy-elegant-pegasus,inst-e06o7-elegant-pegasus,inst-e7jdm-elegant-pegasus,inst-eg5rm-elegant-pegasus,inst-evudf-elegant-pegasus,inst-faunu-elegant-pegasus,inst-ieq5l-elegant-pegasus,inst-ixm8o-elegant-pegasus,inst-kwjdc-elegant-pegasus,inst-n8ztw-elegant-pegasus,inst-nmggj-elegant-pegasus,inst-pvwsj-elegant-pegasus,inst-qsk26-elegant-pegasus,inst-qukiw-elegant-pegasus,inst-rt9cr-elegant-pegasus,inst-szfph-elegant-pegasus,inst-t6h37-elegant-pegasus,inst-xl6if-elegant-pegasus,inst-z2ukx-elegant-pegasus"
ORACLE_HOSTS = {k: i for i,k in enumerate(SLURMLIST.split(','))}
import numpy as np
def _get_host_id():
    hostname = socket.gethostname()
    try:
        return ORACLE_HOSTS[hostname]
    except KeyError:
        raise KeyError(f'hostname {hostname} not in {ORACLE_HOSTS}')

# commoncrawl.org
CC_BASE_URL = 'https://commoncrawl.s3.amazonaws.com/'
DATA_DIR = "s3://character-ai-eu-frankfurt-1/checkpoints/sam/cc_news"
ARTICLE_DIR = os.path.join(DATA_DIR, 'articles')
# log file of fully extracted WARC files
n_warc_files_on_cc = 0


__counter_article_passed = 0
__counter_article_discarded = 0
__counter_article_error = 0
__counter_article_total = 0
__counter_warc_skipped = 0
__counter_warc_processed = 0
__start_time = time.time()


__common_crawl_start_date = datetime.datetime(2016, 8, 26) # When Common Crawl started.
configure_logging({"LOG_LEVEL": "ERROR"})
logging.getLogger('requests').setLevel(logging.CRITICAL)
logging.getLogger('readability').setLevel(logging.CRITICAL)
logging.getLogger('PIL').setLevel(logging.CRITICAL)
logging.getLogger('newspaper').setLevel(logging.CRITICAL)
logging.getLogger('newsplease').setLevel(logging.CRITICAL)
logging.getLogger('urllib3').setLevel(logging.CRITICAL)
logging.getLogger('jieba').setLevel(logging.CRITICAL)


def __iterate_by_month(start_date=None, end_date=None, month_step=1):
    if start_date is None:
        # The starting month of Common Crawl.
        start_date = __common_crawl_start_date
    if end_date is None:
        # Until now.
        end_date = datetime.datetime.today()
    current_date = start_date
    while current_date < end_date:
        yield current_date
        carry, new_month = divmod(current_date.month - 1 + month_step, 12)
        new_month += 1
        current_date = current_date.replace(year=current_date.year + carry,
                                            month=new_month)


def __extract_date_from_warc_filename(path):
    fn = os.path.basename(path)
    # Assume the filename pattern is CC-NEWS-20160911145202-00018.warc.gz
    fn = fn.replace('CC-NEWS-', '')
    dt = fn.split('-')[0]
    try:
        return datetime.datetime.strptime(dt, '%Y%m%d%H%M%S')
    except ValueError as e:
        #raise ValueError(f'Could not convert fn={fn},dt={dt}')
        return None #20210327090019


def __date_within_period(date, start_date=None, end_date=None):
    if date is None: return False
    if start_date is None:
        # The starting month of Common Crawl.
        start_date = __common_crawl_start_date
    if end_date is None:
        # Until now.
        end_date = datetime.datetime.today()
    return start_date <= date < end_date


def get_remote_index(warc_files_start_date, warc_files_end_date, temp_filename = 'cc.index'):
    """Gets the index of news crawl files from commoncrawl.org and returns an array of names
    """
    cmd = ''
    if warc_files_start_date or warc_files_end_date:
        # cleanup
        try:
            os.remove(temp_filename)
        except OSError:
            pass

        # The news files are grouped per year and month in separate folders
        warc_dates = __iterate_by_month(start_date=warc_files_start_date, end_date=warc_files_end_date)
        for date in warc_dates:
            year = date.strftime('%Y')
            month = date.strftime('%m')
            cmd += "aws s3 ls --recursive s3://commoncrawl/crawl-data/CC-NEWS/%s/%s/ --no-sign-request >> %s && " % (year, month, temp_filename)

    else:
        cmd = "aws s3 ls --recursive s3://commoncrawl/crawl-data/CC-NEWS/ --no-sign-request > %s && " % temp_filename
    awk_parameter = '"{ print $4 }"' if os.name == 'nt' else "'{ print $4 }'"
    cmd += f"awk {awk_parameter} {temp_filename} "
    logger.info('executing: %s', cmd)
    exitcode, stdout_data = subprocess.getstatusoutput(cmd)

    if exitcode > 0:
        raise Exception(stdout_data)

    lines = stdout_data.splitlines()

    if warc_files_start_date or warc_files_end_date:
        # Now filter further on day of month, hour, minute
        lines = [
            p for p in lines if __date_within_period(
                __extract_date_from_warc_filename(p),
                start_date=warc_files_start_date,
                end_date=warc_files_end_date,
            )
        ]
    os.remove(temp_filename)
    return lines


def get_fully_extracted_warc_urls(log_file):
    """Reads in the log file that contains a list of all previously, fully extracted WARC urls"""
    if not os.path.isfile(log_file):
        return []
    with open(log_file) as log:
        return [x.strip().split(',')[1] for x in log.readlines()]

def set_creds():
    rclone_config = json.loads(sh.rclone("config", "dump").stdout)
    oci_cfg = rclone_config["oci"]
    os.environ["AWS_SECRET_ACCESS_KEY"] = oci_cfg["secret_access_key"]
    os.environ["AWS_ACCESS_KEY_ID"] = oci_cfg["access_key_id"]
    os.environ["AZURE_STORAGE_ACCOUNT_NAME"] = rclone_config["azure"]["account"]
    os.environ["AZURE_STORAGE_ACCOUNT_KEY"] = rclone_config["azure"]["key"]


set_creds()
def finished_warc_callback(warc_path, counter_article_passed, counter_article_discarded, counter_article_error,
                           counter_article_total):
    """Internal callback on completion of one WARC file. Calculating some statistics on processing speed."""
    global __counter_warc_processed
    # global __counter_warc_skipped
    elapsed_secs = time.time() - __start_time
    __counter_warc_processed += 1

    sec_per_article = elapsed_secs / counter_article_total
    h_per_warc = elapsed_secs / __counter_warc_processed / 3600
    remaining_warcs = n_warc_files_on_cc - (__counter_warc_processed + __counter_warc_skipped)

    logger.info("warc processing statistics")
    logger.info("warc files skipped = %i, processed = %i, remaining = %i, total = %i", __counter_warc_skipped,
                __counter_warc_processed, remaining_warcs, n_warc_files_on_cc)
    logger.info("global [s/article] = %f", sec_per_article)
    logger.info("global [h/warc] = %.3f", h_per_warc)
    logger.info("estimated remaining time [h] = %f", remaining_warcs / h_per_warc)


import warcio
from warcio.exceptions import ArchiveLoadFailed
def extract(warc_url,  **kwargs):
    cce = CommonCrawlExtractor(**kwargs)
    try:
        cce.process_warc_gz_url(warc_url)
    except ArchiveLoadFailed:
        cce.save_status(warc_url, 0)


def crawl_from_commoncrawl(valid_hosts=None,
                           logfile=None,
                           start_date=None, end_date=None, warc_files_start_date=None, warc_files_end_date=None,
                           strict_date=True, reuse_previously_downloaded_files=True, save_dir=None,
                           continue_after_error=True, show_download_progress=False, nproc=4, log_level=logging.ERROR,
                           delete_warc_after_extraction=True, continue_process=True, fetch_images=False,
                           process_hardcoded_partition=True):
    """
    Crawl and extract articles form the news crawl provided by commoncrawl.org. For each article that was extracted
    successfully the callback function callback_on_article_extracted is invoked where the first parameter is the
    article object.
    """


    done_urls = get_fully_extracted_warc_urls(logfile) if logfile is not None else []
    urls_to_process = get_urls_to_process(continue_process, nproc, warc_files_end_date, warc_files_start_date, done_urls)

    kwargs = dict(save_dir=save_dir,
                  callback_on_warc_completed=finished_warc_callback,
                  start_date=start_date, end_date=end_date,
                  strict_date=strict_date,
                  reuse_previously_downloaded_files=reuse_previously_downloaded_files,
                  continue_after_error=continue_after_error,
                  show_download_progress=show_download_progress,
                  log_level=log_level,
                  delete_warc_after_extraction=delete_warc_after_extraction,
                  fetch_images=fetch_images)

    if nproc > 1:
        with Pool(nproc) as proc_pool:
            map_fn = partial(extract, **kwargs)
            proc_pool.map(map_fn, urls_to_process)
    else:
        for url in urls_to_process:
            extract(url, **kwargs)


def get_urls_to_process(continue_process, nproc, warc_files_end_date, warc_files_start_date, fully_extracted_warc_urls):
    cc_news_crawl_names = get_remote_index(warc_files_start_date, warc_files_end_date)
    global n_warc_files_on_cc
    n_warc_files_on_cc = len(cc_news_crawl_names)
    # multiprocessing (iterate the list of crawl_names, and for each: download and process it)
    logger.info('creating extraction process pool with %i processes', nproc)
    warc_download_urls = []

    logger.info(f'found {n_warc_files_on_cc} files at commoncrawl.org {len(fully_extracted_warc_urls)} locally')
    for name in cc_news_crawl_names:
        url = CC_BASE_URL + name
        if continue_process:
            # check if the current WARC has already been fully extracted (assuming that the filter criteria have not
            # been changed!)
            if url in fully_extracted_warc_urls:
                logger.info('skipping WARC because fully extracted: %s' % url)
                global __counter_warc_skipped
                __counter_warc_skipped += 1
                pass
            else:
                warc_download_urls.append(url)

        else:
            # if not continue process, then always add
            warc_download_urls.append(url)
    # raise ValueError(f'n_warc_download_urls={len(warc_download_urls)}')
    # run the crawler in the current, single process if number of extraction processes is set to 1
    try:
        urls_to_process = get_urls_to_process_oracle(warc_download_urls)
    except KeyError:
        urls_to_process = warc_download_urls
    return urls_to_process


def get_urls_to_process_oracle(warc_download_urls):
    node_id = _get_host_id()
    n_nodes = len(ORACLE_HOSTS)  # number of nodes
    chunks = np.array_split(warc_download_urls, n_nodes)
    chunk_lens = [len(x) for x in chunks]
    assert len(chunk_lens) == n_nodes
    assert sum(chunk_lens) == len(warc_download_urls), f'{sum(chunk_lens)} != {len(warc_download_urls)}'
    urls_to_process = chunks[node_id]
    return urls_to_process

logging.getLogger("botocore.credentials").setLevel(logging.WARNING)  # Much noise

logging.getLogger("sh").setLevel(logging.WARNING)  # Much noise




def main(nproc=64, save_dir=ARTICLE_DIR, logfile='/home/sam/cc_news_url_status.log'):
    """"""
    # example valid_hosts ['elrancaguino.cl'] # hosts (if None or empty list, any host is OK)

    # datetime.datetime(2016, 1, 1) # start date (if None, any date is OK as start date), as datetime
    if nproc == 1:
        files_since = datetime.datetime(2021, 3, 1, 12, 30)
        files_before = datetime.datetime(2021, 3, 15, 12, 55)
    else:
        files_since, files_before = None, None
    start_date, end_date = None, None
    # if date filtering is strict and news-please could not detect the date of an article, the article will be discarded
    my_filter_strict_date = False
    # if True, the script checks whether a file has been downloaded already and uses that file instead of downloading
    # again. Note that there is no check whether the file has been downloaded completely or is valid!
    my_reuse_previously_downloaded_files = True
    continue_after_error = True  # continue after error
    show_download_progress = True  # show the progress of downloading the WARC files
    json_style = 0  # 0 (minimize), 1 (pretty)
    my_delete_warc_after_extraction = True  # if True, the WARC file will be deleted after all articles have been extracted from it
    my_continue_process = True  # if True, will continue extraction from the latest fully downloaded but not fully extracted WARC files and then crawling new WARC files.
    my_fetch_images = False
    ############ END YOUR CONFIG #########
    crawl_from_commoncrawl(valid_hosts=[],
                           start_date=start_date,
                           end_date=end_date,
                           warc_files_start_date=files_since,
                           warc_files_end_date=files_before,
                           strict_date=my_filter_strict_date,
                           reuse_previously_downloaded_files=my_reuse_previously_downloaded_files,
                           save_dir=save_dir,
                           continue_after_error=continue_after_error,
                           show_download_progress=show_download_progress,
                           nproc=nproc,
                           log_level=log_level,
                           delete_warc_after_extraction=my_delete_warc_after_extraction,
                           continue_process=True,
                           logfile=logfile,
                           fetch_images=my_fetch_images)


from fire import Fire
if __name__ == "__main__":
    Fire(main)

