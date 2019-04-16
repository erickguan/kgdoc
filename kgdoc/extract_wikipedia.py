import sys
import lazynlp
from collections import defaultdict
import traceback
from urllib.parse import urlparse
import wikipediaapi
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor


DB_NAME = 'test.db'# 'download_urls.db'

def load_wikipedia_list(filename):
  lazynlp.dedup_lines_from_new_file([filename], new_file, outfile)

def extract_qid(filename):
  import sqlite3
  conn = sqlite3.connect(DB_NAME)
  c = conn.cursor()

  c.execute('''CREATE TABLE IF NOT EXISTS urls
              (qid text, site text, url text);''')
  c.execute('''CREATE INDEX IF NOT EXISTS urls_idx_on_qid ON urls(qid);''')
  c.execute('''CREATE INDEX IF NOT EXISTS urls_idx_on_qid_site ON urls(qid, site);''')
  conn.commit()

  available_sites = set(["en", "zh", "sv"])

  with open(filename, 'r') as f:
    for l in f:
      try:
        s = l.rstrip().split('\t')
        q_id, url = s
        site = url[8:].split('.')[0]
        if site in available_sites:
          c.execute(f"INSERT INTO urls VALUES ('{q_id}','{site}','{url}')")
      except:
        print(traceback.print_exc())
        continue

def download_wp_url(url):
  # https://en.wikipedia.org/wiki/Maltolt
  url_s = urlparse(url)
  site = url_s.netloc.split('.')[0]
  wiki = wikipediaapi.Wikipedia(site, extract_format=wikipediaapi.ExtractFormat.WIKI)
  page_name = url_s.path[6:]
  page = wiki.page(page_name)
  return page

def download_summary(url):
  page = download_wp_url(url)
  return page.summary

def populate_summary(r):
  qid, site, url = r
  try:
    summary = download_summary(url)
  except:
    summary = ""
  return qid, site, summary

def batch_download_summary(db_name, batch):
  import sqlite3

  with ThreadPoolExecutor(max_workers=1) as t:
    print(batch)
    res = [r for r in t.map(populate_summary, batch)]

    print(res)
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    for qid, site, summary in res:
      c.execute('''INSERT INTO summaries VALUES (?, ?, ?);''', (qid, site, summary))
    conn.commit()
    conn.close()

def download_qid_summary():
  import sqlite3
  conn = sqlite3.connect(DB_NAME)
  c = conn.cursor()

  c.execute('''CREATE TABLE IF NOT EXISTS summaries
              (qid text, site text, summary text);''')
  c.execute('''CREATE INDEX IF NOT EXISTS summaries_idx_on_qid ON summaries(qid);''')
  c.execute('''CREATE INDEX IF NOT EXISTS summaries_idx_on_qid_site ON summaries(qid, site);''')
  conn.commit()

  c.execute('SELECT * FROM urls;')

  # with ProcessPoolExecutor(max_workers=1) as p:
  r = [0]
  while len(r) != 0:
    r = c.fetchmany()
    batch_download_summary(DB_NAME, r)
      # fut.result()
  conn.close()


if __name__ == '__main__':
  print(sys.argv)
  if sys.argv[1] == "extract_qid":
    extract_qid(sys.argv[2])
  elif sys.argv[1] == "download_summary":
    download_summary(sys.argv[2])
  elif sys.argv[1] == "summary_for_qid":
    download_qid_summary()
  else:
    load_wikipedia_list(sys.argv[1])
