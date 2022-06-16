#!/usr/bin/env python
"""
This scripts downloads WARC files from commoncrawl.org's news crawl and extracts articles from these files. You can
define filter criteria that need to be met (see YOUR CONFIG section), otherwise an article is discarded. Currently, the
script stores the extracted articles in JSON files, but this behaviour can be adapted to your needs in the method
on_valid_article_extracted. To speed up the crawling and extraction process, the script supports multiprocessing. You can
control the number of processes with the parameter nproc.

You can also crawl and extract articles programmatically, i.e., from within
your own code, by using the class CommonCrawlCrawler or the function
commoncrawl_crawler.crawl_from_commoncrawl(...) provided in
newsplease.crawler.commoncrawl_crawler.py. In this case there is also the
possibility of passing in a your own subclass of CommonCrawlExtractor as
extractor_cls=... . One use case here is that your subclass can customise
filtering by overriding `.filter_record(...)`.

In case the script crashes and contains a log message in the beginning that states that only 1 file on AWS storage
was found, make sure that awscli was correctly installed. You can check that by running aws --version from a terminal.
If aws is not installed, you can (on Ubuntu) also install it using sudo apt-get install awscli.

This script uses relative imports to ensure that the latest, local version of news-please is used, instead of the one
that might have been installed with pip. Hence, you must run this script following this workflow.
git clone https://github.com/fhamborg/news-please.git
cd news-please
python3 -m newsplease.examples.commoncrawl

Note that by default the script does not extract main images since they are not contained
WARC files. You can enable extraction of main images by setting `my_fetch_images=True`
"""

