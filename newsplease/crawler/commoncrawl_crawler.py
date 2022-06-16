#!/usr/bin/env python
"""
Provides functionality to crawl and extract news articles from commoncrawl.org. Filter criteria, such as publish date
and host list, can be defined. Currently, all WARC files will be downloaded to the path_or_url WORKINGDIR/cc_download_warc, if
not otherwise specified.
"""
from functools import partial
from multiprocessing import Pool

from newsplease.crawler.cc_urls import n_warc_files_on_cc, read_completed_urls, set_creds, s3_ls, get_unprocessed_urls, \
    N_WARC_SKIPPED
from ..crawler.commoncrawl_extractor import CommonCrawlExtractor, less_noise

import logging
import os
import datetime
import time
import jsonlines
from filelock import FileLock
import re
import socket
from warcio.exceptions import ArchiveLoadFailed
from pathlib import Path

logger = logging.getLogger(__name__)
log_level = logging.INFO
logging.basicConfig(level=log_level)
less_noise()

N_WARC_COMPLETED = 0
GLOBAL_START_TIME = time.time()

set_creds()




def get_log_df(log_file):
    import pandas as pd
    records = []
    with jsonlines.open(log_file, 'r') as reader:
        for record in reader:
            records.append(record)
    return pd.DataFrame(records)



def finished_warc_callback(warc_path, counter_article_passed, counter_article_discarded, counter_article_error,
                           counter_article_total):
    """Internal callback on completion of one WARC file. Calculating some statistics on processing speed."""
    global N_WARC_COMPLETED
    # global N_WARC_SKIPPED
    elapsed_secs = time.time() - GLOBAL_START_TIME
    N_WARC_COMPLETED += 1

    sec_per_article = elapsed_secs / counter_article_total
    h_per_warc = elapsed_secs / N_WARC_COMPLETED / 3600
    remaining_warcs = n_warc_files_on_cc - (N_WARC_COMPLETED + N_WARC_SKIPPED)

    host = socket.gethostname()
    print(f'hostname={host}, {warc_path}')
    logger.info("warc files skipped = %i, processed = %i, remaining = %i, total = %i", N_WARC_SKIPPED,
                N_WARC_COMPLETED, remaining_warcs, n_warc_files_on_cc)
    logger.info("global [s/article] = %f", sec_per_article)
    logger.info("global [h/warc] = %.3f", h_per_warc)
    logger.info("estimated remaining time [h] = %f", remaining_warcs / h_per_warc)


def remove_suffix(text: str, suffix: str):
    if text.endswith(suffix):
        return text[:-len(suffix)]
    return text  # or whatever




def last_record_from_fname(full_s3_path):
    fname = os.path.basename(full_s3_path)
    match = re.search(r'last_record_(\d+).json', fname)
    if match is not None:
        return int(match.groups()[0])
    return 0

# basenames like last_record_40138.json
def infer_resume_idx(full_save_dir):
    contents = s3_ls(full_save_dir)
    if not contents:
        return 0
    else:
        return max(last_record_from_fname(x) for x in contents)



def extract(url, logfile, stop_early=False, flush_frequency=10000, **kwargs):
    assert url.endswith('warc.gz')
    save_dir = kwargs['save_dir']
    start_time = time.strftime('%Y-%m-%d-%H:%M:%S')
    cce = CommonCrawlExtractor(**kwargs)
    save_subdir = remove_suffix(url.split('crawl-data/')[1], '.warc.gz')
    assert 'https' not in save_subdir, save_subdir
    assert '.com' not in save_subdir, save_subdir
    full_save_dir = os.path.join(save_dir, save_subdir)
    resume_idx = infer_resume_idx(full_save_dir)
    # like s3://datasets/cc_news/mar31/CC-NEWS/2021/03/CC-NEWS-20210307190521-00595
    warcio_error = False
    completed = False
    n_files = 0
    try:
        n_files = cce.process_warc_gz_url(url, full_save_dir, stop_early=stop_early, flush_frequency=flush_frequency, resume_idx=resume_idx)
        completed = not stop_early
    except ArchiveLoadFailed:
        warcio_error = True
    # dont rename fields, ideally.
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
    min_date = datetime.date(2019,1, 1)
    max_date = None

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


logging.getLogger("botocore.credentials").setLevel(logging.WARNING)  # Much noise
logging.getLogger("sh").setLevel(logging.WARNING)  # Much noise

from fire import Fire
if __name__ == "__main__":
    Fire(main)

