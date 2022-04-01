import datetime
import json
import os
import socket
import subprocess
import logging
import numpy as np
import sh
import jsonlines
import sh

SLURMLIST="inst-0kygf-elegant-pegasus,inst-1peuj-elegant-pegasus,inst-4ilmp-elegant-pegasus,inst-4mhhw-elegant-pegasus,inst-6utfo-elegant-pegasus,inst-8cas1-elegant-pegasus,inst-bm1tl-elegant-pegasus,inst-c3vih-elegant-pegasus,inst-ceia2-elegant-pegasus,inst-cu41e-elegant-pegasus,inst-czlp2-elegant-pegasus,inst-dxwdy-elegant-pegasus,inst-e06o7-elegant-pegasus,inst-e7jdm-elegant-pegasus,inst-eg5rm-elegant-pegasus,inst-evudf-elegant-pegasus,inst-faunu-elegant-pegasus,inst-ieq5l-elegant-pegasus,inst-ixm8o-elegant-pegasus,inst-kwjdc-elegant-pegasus,inst-n8ztw-elegant-pegasus,inst-nmggj-elegant-pegasus,inst-pvwsj-elegant-pegasus,inst-qsk26-elegant-pegasus,inst-qukiw-elegant-pegasus,inst-rt9cr-elegant-pegasus,inst-szfph-elegant-pegasus,inst-t6h37-elegant-pegasus,inst-xl6if-elegant-pegasus,inst-z2ukx-elegant-pegasus"
ORACLE_HOSTS = {k: i for i,k in enumerate(SLURMLIST.split(','))}
CC_BASE_URL = 'https://commoncrawl.s3.amazonaws.com/'
N_WARC_SKIPPED = 0
logger = logging.getLogger(__name__)
log_level = logging.INFO
logging.basicConfig(level=log_level)

from pathlib import Path
ROOT_DIR = Path(__file__).parent.parent.parent
assert ROOT_DIR.name == 'news-please', ROOT_DIR
INDEX_FILE = os.path.join(ROOT_DIR, 'cc_full.index')

CC_START_DATE = datetime.datetime(2016, 8, 26).date() # When Common Crawl started.
TODAY = datetime.datetime.today().date()
def _get_host_id():
    hostname = socket.gethostname()
    try:
        return ORACLE_HOSTS[hostname]
    except KeyError:
        raise KeyError(f'hostname {hostname} not in {ORACLE_HOSTS}')


n_warc_files_on_cc = 0

#
def get_remote_index(min_date=CC_START_DATE, max_date=TODAY, index_file =INDEX_FILE):
    """Gets the index of news crawl files from commoncrawl.org and returns an array of names
    """
    if max_date is None: max_date = TODAY
    if min_date is None: min_date = CC_START_DATE
    if os.path.exists(index_file):
        lines = [x.strip() for x in open(index_file, 'r').readlines()]
        print(f'Read {len(lines)} from {index_file}')
    else:
        awk_parameter = '"{ print $4 }"' if os.name == 'nt' else "'{ print $4 }'"
        cmd = f"aws s3 ls --recursive s3://commoncrawl/crawl-data/CC-NEWS/ --no-sign-request | {awk_parameter} > {INDEX_FILE}"
        logger.info('executing: %s', cmd)
        exitcode, stdout_data = subprocess.getstatusoutput(cmd)
        if exitcode > 0: raise Exception(stdout_data)
        lines = stdout_data.splitlines()

    def date_check(p) -> bool:
        path_date = fname_to_date(p)
        return path_date is not None and min_date <= path_date <= max_date

    return [p for p in lines if date_check(p)]



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


def s3_ls(path):
    results = sorted(
        [c.strip("/\n") for c in sh.rclone.lsf(path)]
    )
    #results = subprocess.run(cmd, capture_output=True)
    return results


def get_unprocessed_urls(processed_urls, min_date, max_date):

    global n_warc_files_on_cc
    global N_WARC_SKIPPED
    cc_news_crawl_names = get_remote_index(min_date, max_date)
    n_warc_files_on_cc = len(cc_news_crawl_names)
    warc_download_urls = []
    logger.info(f'found {n_warc_files_on_cc} files at commoncrawl.org {len(processed_urls)} locally')
    for name in cc_news_crawl_names:
        if not name.endswith('warc.gz'): continue
        url = CC_BASE_URL + name
        if url in processed_urls:
            logger.info(f'skipping WARC because fully extracted: {url}')
            N_WARC_SKIPPED += 1
            continue
        warc_download_urls.append(url)
    try:
        node_id = _get_host_id()
        chunks = chunk_urls_for_oracle(warc_download_urls)
        urls_to_process = chunks[node_id]
    except KeyError:
        urls_to_process = warc_download_urls

    return list(reversed(sorted(urls_to_process))) # newer first


def chunk_urls_for_oracle(url_list):
    n_nodes = len(ORACLE_HOSTS)  # number of nodes
    chunks = np.array_split(url_list, n_nodes)
    chunk_lens = [len(x) for x in chunks]
    assert len(chunk_lens) == n_nodes
    assert sum(chunk_lens) == len(url_list), f'{sum(chunk_lens)} != {len(url_list)}'
    return chunks


def fname_to_date(path_or_url):
    fn = os.path.basename(path_or_url)
    # Assume the filename pattern is CC-NEWS-20160911145202-00018.warc.gz
    fn = fn.replace('CC-NEWS-', '')
    dt = fn.split('-')[0]
    try:
        return datetime.datetime.strptime(dt, '%Y%m%d%H%M%S').date()
    except ValueError as e:
        return None #20210327090019
