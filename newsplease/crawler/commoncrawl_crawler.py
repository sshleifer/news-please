#!/usr/bin/env python
"""
Provides functionality to crawl and extract news articles from commoncrawl.org. Filter criteria, such as publish date
and host list, can be defined. Currently, all WARC files will be downloaded to the path_or_url WORKINGDIR/cc_download_warc, if
not otherwise specified.
"""
import subprocess
import time
from functools import partial
from multiprocessing import Pool
from newsplease.crawler.commoncrawl_extractor import fname_to_date
from scrapy.utils.log import configure_logging

from ..crawler.commoncrawl_extractor import CommonCrawlExtractor, less_noise

import json
import logging
import os
import datetime

import sh
from warcio.exceptions import ArchiveLoadFailed
from pathlib import Path
ROOT_DIR = Path(__file__).parent.parent.parent
assert ROOT_DIR.name == 'news-please', ROOT_DIR
INDEX_FILE = os.path.join(ROOT_DIR, 'cc_full.index')

logger = logging.getLogger(__name__)
log_level = logging.INFO
logging.basicConfig(level=log_level)
less_noise()
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
# log file of fully extracted WARC files
n_warc_files_on_cc = 0
__counter_warc_skipped = 0
__counter_warc_processed = 0
__start_time = time.time()


CC_START_DATE = datetime.datetime(2016, 8, 26) # When Common Crawl started.


def __iterate_by_month(start_date=None, end_date=None, month_step=1):
    if start_date is None:
        start_date = CC_START_DATE
    if end_date is None:
        end_date = datetime.datetime.today()
    current_date = start_date
    while current_date < end_date:
        yield current_date
        carry, new_month = divmod(current_date.month - 1 + month_step, 12)
        new_month += 1
        current_date = current_date.replace(year=current_date.year + carry, month=new_month)


def _is_date_within_period(date, start_date=None, end_date=None) -> bool:
    if date is None: return False
    if start_date is None:
        # The starting month of Common Crawl.
        start_date = CC_START_DATE
    if end_date is None:
        # Until now.
        end_date = datetime.datetime.today()
    return start_date <= date < end_date

# aws s3 ls --recursive s3://commoncrawl/crawl-data/CC-NEWS/ --no-sign-request | awk '{ print $4 }' > cc_full.index
def get_remote_index(min_date, max_date, temp_filename ='cc.index'):
    """Gets the index of news crawl files from commoncrawl.org and returns an array of names
    """
    cmd = "aws s3 ls --recursive s3://commoncrawl/crawl-data/CC-NEWS/ --no-sign-request > %s && " % temp_filename
    awk_parameter = '"{ print $4 }"' if os.name == 'nt' else "'{ print $4 }'"
    cmd += f"awk {awk_parameter} {temp_filename} "
    logger.info('executing: %s', cmd)
    exitcode, stdout_data = subprocess.getstatusoutput(cmd)

    if exitcode > 0:
        raise Exception(stdout_data)

    lines = stdout_data.splitlines()

    if min_date or max_date:
        # Now filter further on day of month, hour, minute
        lines = [
            p for p in lines if _is_date_within_period(
                fname_to_date(p),
                start_date=min_date,
                end_date=max_date,
            )
        ]
    #os.remove(temp_filename)
    return lines


def read_completed_urls(log_file):
    """Reads in the log file that contains a list of all previously, fully extracted WARC urls"""
    completed_urls = []
    if not os.path.exists(log_file):
        return completed_urls
    with jsonlines.open(log_file, 'r') as reader:
        for record in reader:
            if record.get('completed', False):
                completed_urls.append(record['url'])
    return completed_urls


def set_creds():
    rclone_config = json.loads(sh.rclone("config", "dump").stdout)
    oci_cfg = rclone_config["oci"]
    os.environ["AWS_SECRET_ACCESS_KEY"] = oci_cfg["secret_access_key"]
    os.environ["AWS_ACCESS_KEY_ID"] = oci_cfg["access_key_id"]
    os.environ["AZURE_STORAGE_ACCOUNT_NAME"] = rclone_config["azure"]["account"]
    os.environ["AZURE_STORAGE_ACCOUNT_KEY"] = rclone_config["azure"]["key"]

import socket
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

    host = socket.gethostname()
    print(f'hostname={host}, {warc_path}')
    logger.info("warc files skipped = %i, processed = %i, remaining = %i, total = %i", __counter_warc_skipped,
                __counter_warc_processed, remaining_warcs, n_warc_files_on_cc)
    logger.info("global [s/article] = %f", sec_per_article)
    logger.info("global [h/warc] = %.3f", h_per_warc)
    logger.info("estimated remaining time [h] = %f", remaining_warcs / h_per_warc)


def remove_suffix(text: str, suffix: str):
    if text.endswith(suffix):
        return text[:-len(suffix)]
    return text  # or whatever

import time
import jsonlines
from filelock import FileLock

def extract(url, logfile, stop_early=False, **kwargs):
    assert url.endswith('warc.gz')
    save_dir = kwargs['save_dir']
    start_time = time.strftime('%Y-%m-%d-%H:%M:%S')
    cce = CommonCrawlExtractor(**kwargs)
    save_subdir = remove_suffix(url.split('crawl-data/')[1], '.warc.gz')
    assert 'https' not in save_subdir, save_subdir
    assert '.com' not in save_subdir, save_subdir
    full_save_dir = os.path.join(save_dir, save_subdir)
    # like s3://datasets/cc_news/mar31/CC-NEWS/2021/03/CC-NEWS-20210307190521-00595
    warcio_error = False
    completed = False
    n_files = 0
    try:
        n_files = cce.process_warc_gz_url(url, full_save_dir, stop_early=stop_early)
        completed = not stop_early
    except ArchiveLoadFailed:
        warcio_error = True

    data = dict(url=url, full_save_dir=full_save_dir, completed=completed,
                warcio_error=warcio_error,
                nfiles=n_files, hostname=socket.gethostname(),
                start_ts=start_time,
                end_ts=time.strftime('%Y-%m-%d-%H:%M:%S'))
    #data_str = json.dumps(data)

    lock = FileLock('/home/sam/cc_news.lock')

    with lock.acquire():
        with jsonlines.open(logfile, mode='a') as writer:
            writer.write(data)




def main(nproc=64, save_dir="s3://datasets/cc_news/v1", logfile='/home/sam/v1/cc_news_url_status.jsonl', stop_early=False):
    """"""
    # example valid_hosts ['elrancaguino.cl'] # hosts (if None or empty list, any host is OK)
    # datetime.datetime(2016, 1, 1) # start date (if None, any date is OK as start date), as datetime
    # if nproc == 1:
    #     files_since = datetime.datetime(2021, 3, 1, 12, 30)
    #     files_before = datetime.datetime(2021, 3, 15, 12, 55)
    #
    #
    min_date = datetime.date(2020,3, 1)
    max_date = datetime.date(2022,3, 1)

    done_urls = read_completed_urls(logfile)
    urls_to_process = get_unprocessed_urls(done_urls, min_date, max_date)
    logger.info(f'{len(urls_to_process)} on this worker')
    #if stop_early: urls_to_process  = urls_to_process[:nproc]
    kwargs = dict(logfile=logfile,
                  save_dir=save_dir,
                  callback_on_warc_completed=finished_warc_callback,
                  stop_early=stop_early)

    if nproc > 1:
        with Pool(nproc) as proc_pool:
            map_fn = partial(extract, **kwargs)
            proc_pool.map(map_fn, urls_to_process)
    else:
        from tqdm import tqdm
        for url in tqdm(urls_to_process):
            extract(url, **kwargs)
            if stop_early:
                return



def get_unprocessed_urls(processed_urls, min_date, max_date):
    cc_news_crawl_names = get_remote_index(min_date, max_date)
    #cc_news_crawl_names = [x.strip() for x in open('cc_full.index').readlines()]
    global n_warc_files_on_cc
    n_warc_files_on_cc = len(cc_news_crawl_names)
    # multiprocessing (iterate the list of crawl_names, and for each: download and process it)

    warc_download_urls = []

    logger.info(f'found {n_warc_files_on_cc} files at commoncrawl.org {len(processed_urls)} locally')
    for name in cc_news_crawl_names:
        if not name.endswith('warc.gz'): continue

        url = CC_BASE_URL + name
        if url in processed_urls:
            logger.info(f'skipping WARC because fully extracted: {url}')
            global __counter_warc_skipped
            __counter_warc_skipped += 1
            pass
        else:
            warc_download_urls.append(url)
    try:
        urls_to_process = get_urls_to_process_oracle(warc_download_urls)
    except KeyError:
        urls_to_process = warc_download_urls

    return list(reversed(sorted(urls_to_process))) # newer first


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

from fire import Fire
if __name__ == "__main__":
    Fire(main)

