from .commoncrawl_crawler import (
    extract, infer_resume_idx,
    get_log_df
)
from newsplease.crawler.cc_urls import (
    ORACLE_HOSTS, get_remote_index, read_completed_urls, s3_ls, fname_to_date, get_unprocessed_urls,
    chunk_urls_for_oracle, SLURMLIST
)

from newsplease.crawler.cc_urls import get_remote_index
import datetime

min_date = datetime.date(2020, 6, 1)
max_date = datetime.date(2022, 3, 1)
#print(len(get_remote_index(min_date, max_date)))

LOGFILE = '/home/sam/v1/cc_news_url_status.jsonl'

def print_resume_cmds():
    print(len(ORACLE_HOSTS))
    for h in ORACLE_HOSTS:
        cmd = f'sleep 5s && ssh {h} bash /home/sam/news-please/launcher3.sh &'
        print(cmd)

def test_resume_idx():
    save_dir = 's3://datasets/cc_news/v1/CC-NEWS/2020/05/CC-NEWS-20200502035334-01903'
    s3_ls(save_dir)
    resume_idx = infer_resume_idx(save_dir)
    desired_resume_idx = 79654
    assert resume_idx == desired_resume_idx

def test_extract():
    logfile = "/home/sam/v1/cc_news_url_status.jsonl"
    done_urls = read_completed_urls(LOGFILE)
    todo_urls = get_unprocessed_urls(done_urls, None, None)
    url = todo_urls[0]
    save_dir = 's3://datasets/cc_news/v1'
    extract(url, 'dummy.log', stop_early=True, save_dir=save_dir, flush_frequency=5)


def test_completed_urls_and_logs():
    logfile = "/home/sam/v1/cc_news_url_status.jsonl"
    completed = read_completed_urls(logfile)
    assert len(completed) > 8000
    df =  get_log_df(logfile)
    assert len(set(completed)) == len(completed)
    print(f'len: {len(completed)}, last_few: {completed[-5:]}')


def test_scheduling():
    min_date = datetime.date(2020,3, 1)
    max_date = datetime.date(2022,3, 1)
    warc_urls = get_unprocessed_urls([], min_date, max_date)
    print(len(warc_urls))
    assert len(warc_urls) == len(set(warc_urls))
    chunks = chunk_urls_for_oracle(warc_urls)

def test_next_scheduling():
    min_date = datetime.date(2019,3, 1)
    max_date = None
    done_urls = read_completed_urls(LOGFILE)
    todo_urls = get_unprocessed_urls(done_urls, min_date, max_date)
    chunks = chunk_urls_for_oracle(todo_urls)
    print(f'Done: {len(done_urls)}, Not Done: {len(todo_urls)}')
    chunk_lens = [len(k) for k in chunks]
    print(f'Schedule: n_workers: {len(chunk_lens)} each takes between {min(chunk_lens), max(chunk_lens)}')


import subprocess
def test_generate_command():
    node_list = SLURMLIST.split(',')
    print(f'n_nodes: {len(node_list)}')
    for x in node_list:
        assert x in ORACLE_HOSTS
        cmd = f'sleep 10s && ssh {x} bash /home/sam/news-please/launcher3.sh &'
        print(cmd)
