from bs4 import BeautifulSoup
import argparse
import concurrent.futures
import datetime
import hashlib
import logging
import os
import pandas as pd
import random
import requests
import sys
import time

# Usage:
# Scrape articles for a full year
# python3 medium_scraper.py --year YEAR
# Ex: python3 medium_scraper.py --year 2021
#
# Scrape articles for a selected date range for a year
# python3 medium_scraper.py --year YEAR --start_date START_DATE --end_date END_DATE
# Ex: python3 medium_scraper.py --year 2021 --start_date 01-31 --end_date 02-20

URLS = {
        'Towards Data Science': 'https://towardsdatascience.com/archive/{0}',
        'UX Collective': 'https://uxdesign.cc/archive/{0}',
        'The Startup': 'https://medium.com/swlh/archive/{0}',
        'The Writing Cooperative': 'https://writingcooperative.com/archive/{0}',
        'Data Driven Investor': 'https://medium.datadriveninvestor.com/archive/{0}',
        'Better Humans': 'https://betterhumans.pub/archive/{0}',
        'Better Marketing': 'https://bettermarketing.pub/archive/{0}',
        'Personal Growth' : 'https://medium.com/personal-growth/archive/{0}',
        }
CACHE_DIR = 'cache'
IMAGE_DIR = 'images'
CHECKPOINT_FILE = 'index.txt'


def get_claps(claps_str):
    if (claps_str is None) or (claps_str == '') or (claps_str.split is None):
        return 0
    split = claps_str.split('K')
    claps = float(split[0])
    claps = int(claps*1000) if len(split) == 2 else int(claps)
    return claps

hit_count = 0
total_count = 0

def get_url_content(url, allow_redirects):
    content = b''
    if not url:
        return content
    url_hash = hashlib.sha1(url.encode('utf-8')).hexdigest()
    filename = f"{CACHE_DIR}/{url_hash}"
    global total_count
    global hit_count
    total_count += 1
    if os.path.exists(filename):
        hit_count += 1
        logging.info(f'Cache hit: url={url} filename={url_hash}')
        with open(filename, 'rb') as f:
            return f.read()

    logging.info(f'Cache miss: url={url} filename={url_hash}')
    try:
        response = requests.get(url, allow_redirects=allow_redirects)
    except Exception as e:
        logging.warning(f'Failed to download url={url} exception={e}')
        return content

    if response.status_code != 200:
        logging.warning(f'Failed to download url={url} status_code={response.status_code}')
        if response.status_code == 429:
            sys.exit(f'status_code={response.status_code} Too Many Requests')
        return content

    if response.url.startswith(url):
        content = response.content
    else:
        logging.warning(f'orig_url={url} redirected_to={response.url}')

    with open(filename, 'wb') as f:
        f.write(content)
    return content

def get_img(img_url, dest_filename):
    ext = img_url.split('.')[-1]
    if len(ext) > 4:
        ext = 'jpg'
    dest_filename = f'{dest_filename}.{ext}'
    content = get_url_content(img_url, False)
    if not content:
        logging.warning(f'Failed to download image {img_url}')
        return ''

    with open(f'{IMAGE_DIR}/{dest_filename}', 'wb') as f:
        f.write(content)

    return dest_filename


def find(soup, tag, class_):
    html_tag = soup.find(tag, class_=class_)
    if html_tag is not None:
        return html_tag.contents[0]
    return ''


def get_article_text(article_id, url):
    logging.info(f'scrape_article id={article_id} url={url}')
    if url is None:
        return ('', '', '')
    content = get_url_content(url, True)
    soup = BeautifulSoup(content, 'html.parser')
    title = find(soup, "h1", "pw-post-title")
    subtitle = find(soup, "h2", "pw-subtitle-paragraph")
    first_para = find(soup, "p", "pw-post-body-paragraph")
    return (title, subtitle, first_para)


def scrape_article(article_id, article):
    article_url = article.find_all("a")[3]['href'].split('?')[0]
    title, subtitle, first_para = get_article_text(article_id, article_url)
    # fallback to title from top level page
    if not title:
        title = find(article, 'h3', 'graf--title')
        # title is required field, skip article if title is missing
        if not title:
            logging.warning(f"Failed to parse title for {article_url}. skipping")
            return []
    # fallback to title from top level page
    if not subtitle:
        subtitle = find(article, 'h4', 'graf--subtitle')
    image = article.find("img", class_="graf-image")
    image = '' if image is None else get_img(image['src'], article_id)
    # image is required field, skip article if image is empty
    if not image:
        return []
    buttons = article.find_all("button")
    claps = 0
    if len(buttons) > 1:
        claps = get_claps(buttons[1].contents[0])
    reading_time = article.find("span", class_="readingTime")
    reading_time = 0 if reading_time is None else int(reading_time['title'].split(' ')[0])
    responses = article.find_all("a")
    if len(responses) == 7:
        responses = responses[6].contents[0].split(' ')
        if len(responses) == 0:
            responses = 0
        else:
            responses = responses[0]
    else:
        responses = 0

    return [article_id, article_url, title, subtitle, first_para,
        image, claps, responses, reading_time]


data = []
date_checkpoint = set()

def read_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE) as f:
            for line in f.readlines():
                date_checkpoint.add(line.strip())


def add_to_checkpoint(date):
    date_checkpoint.add(date)
    with open(CHECKPOINT_FILE, 'a') as f:
        f.write(date + '\n')


def scrape_publication(publication, url, date, executor):
    logging.info(f'scrape_publication publication={publication} url={url}')
    if url is None:
        return ('', '', '')
    content = get_url_content(url, True)
    soup = BeautifulSoup(content, 'html.parser')
    articles = soup.find_all(
        "div",
        class_="postArticle postArticle--short js-postArticle js-trackPostPresentation js-trackPostScrolls")
    global data
    artical_num = len(data)
    futures = []
    for article in articles:
        article_id = date.strftime('%Y_%m_%d') + f'_{artical_num}'
        futures.append(executor.submit(scrape_article, article_id, article))
        artical_num += 1

    ext = [publication, date.strftime('%Y/%m/%d')]
    for f in futures:
        res = f.result()
        if len(res) > 0:
            res.extend(ext)
            data.append(res)


def cache_publication(start_date, end_date, executor):
    futures = []
    for d in range((end_date - start_date).days + 1):
        date = start_date + datetime.timedelta(days = d)
        for publication, url in URLS.items():
            futures.append(executor.submit(get_url_content, url.format(date.strftime('%Y/%m/%d')), True))
    for f in futures:
        f.result()
    global total_count
    global hit_count
    total_count = 0
    hit_count = 0


def scrape_data(start_date, end_date, data_file, executor):
    global date_checkpoint
    for d in range((end_date - start_date).days + 1):
        date = start_date + datetime.timedelta(days = d)
        start = time.perf_counter()
        date_str = date.strftime('%Y/%m/%d')
        if date_str in date_checkpoint:
            continue
        for publication, url in URLS.items():
            scrape_publication(publication, url.format(date_str), date, executor)

        global data
        medium_df = pd.DataFrame(data, columns=['id', 'url', 'title', 'subtitle',
            'first_para', 'image', 'claps', 'responses', 'reading_time',
            'publication', 'date'])
        medium_df.to_csv(data_file, index=False, mode='a', header=False)
        global total_count
        global hit_count
        throughput =  int(len(data) / (time.perf_counter() - start) * 60)  # per minute
        cache_hit_rate = int(hit_count / total_count * 100)
        logging.info(f"{date_str} articles={len(data)} throughput={throughput} articles/min cache hit rate={cache_hit_rate}")
        hit_count = 0
        total_count = 0
        add_to_checkpoint(date_str)
        data = []


def parse_arguments():
    parser = argparse.ArgumentParser('Scrape data from medium.')
    parser.add_argument('--year', required=True, type=int,
            help='year to scrape data for')
    parser.add_argument('--start_date', default='01-01', help='start date mm-dd')
    parser.add_argument('--end_date', default='12-31', help='end date mm-dd')
    parser.add_argument('--num_workers', type=int, default=4,
            help='Number of workers for concurrent execution')
    parser.add_argument('--clean', action='store_true', help='clean run')
    return parser.parse_args()


def main():
    args = parse_arguments()
    os.makedirs(CACHE_DIR, exist_ok=True)
    os.makedirs(IMAGE_DIR, exist_ok=True)

    year = args.year
    log_file = f'{year}_medium_scraper.log'
    data_file = f'{year}_medium_data.csv'
    if args.clean:
        if os.path.exists(CHECKPOINT_FILE):
            os.remove(CHECKPOINT_FILE)
        if os.path.exists(data_file):
            os.remove(data_file)
        if os.path.exists(log_file):
            os.remove(log_file)
        medium_df = pd.DataFrame([], columns=['id', 'url', 'title', 'subtitle',
            'first_para', 'image', 'claps', 'responses', 'reading_time',
            'publication', 'date'])
        medium_df.to_csv(data_file, index=False)

    logging.basicConfig(
            format='%(asctime)s %(levelname)s: %(message)s',
            filename=log_file,
            encoding='utf-8',
            level=logging.INFO)
    read_checkpoint()
    executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=args.num_workers)

    start_date = datetime.date.fromisoformat(f'{year}-{args.start_date}')
    end_date = datetime.date.fromisoformat(f'{year}-{args.end_date}')

    cache_publication(start_date, end_date, executor)
    scrape_data(start_date, end_date, data_file, executor)

    executor.shutdown(wait=True, cancel_futures=False)


if __name__ == "__main__":
    main()
