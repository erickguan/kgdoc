import sys
import lazynlp
from collections import defaultdict
import traceback
from urllib.parse import urlparse
import wikipediaapi
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import psycopg2

# psql -U postgres -h 172.19.0.3 -p 5432 -d wikidata
DB_INFO = "dbname='wikidata' user='postgres' host='172.19.0.3' port='5432'"

def load_wikipedia_list(filename):
  lazynlp.dedup_lines_from_new_file([filename], new_file, outfile)

def extract_qid(filename):
  conn = psycopg2.connect(DB_INFO)
  c = conn.cursor()

  c.execute('''CREATE TABLE IF NOT EXISTS urls
              (qid INT, site TEXT, url TEXT);''')
  c.execute('''CREATE INDEX IF NOT EXISTS urls_idx_on_qid ON urls(qid);''')
  c.execute('''CREATE UNIQUE INDEX IF NOT EXISTS urls_idx_on_qid_site ON urls(qid, site);''')
  conn.commit()

  c = conn.cursor()

  available_sites = set(["en", "zh", "sv"])

  i = 0
  with open(filename, 'r') as f:
    for l in f:
      try:
        s = l.rstrip().split('\t')
        q_id, url = s
        site = url[8:].split('.')[0]
        q_id = int(q_id[1:])
        if site in available_sites:
          c.execute("INSERT INTO urls (qid, site, url) VALUES (%s, %s, %s);", (q_id, site, url))
          i += 1
      except:
        print(traceback.print_exc())
        continue

      if i > 1000:
        i = 0
        conn.commit()
        c = conn.cursor()


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
  try:
    with ThreadPoolExecutor(max_workers=32) as t:
      res = [r for r in t.map(populate_summary, batch)]

      conn = psycopg2.connect(DB_INFO)
      c = conn.cursor()
      for qid, site, summary in res:
        c.execute('''INSERT INTO summaries VALUES (%s, %s, %s);''', (qid, site, summary))
      conn.commit()
      conn.close()
  except:
    return

def download_qid_summary():
  conn = psycopg2.connect(DB_INFO)
  c = conn.cursor()

  c.execute('''CREATE TABLE IF NOT EXISTS summaries
              (qid INT, site TEXT, summary TEXT);''')
  c.execute('''CREATE INDEX IF NOT EXISTS summaries_idx_on_qid ON summaries(qid);''')
  c.execute('''CREATE UNIQUE INDEX IF NOT EXISTS summaries_idx_on_qid_site ON summaries(qid, site);''')
  conn.commit()

  c.execute('SELECT * FROM urls;')

  with ProcessPoolExecutor(max_workers=30) as p:
    r = [0]
    while len(r) != 0:
      try:
        r = c.fetchmany()
        p.submit(batch_download_summary, DB_INFO, r)
          # fut.result()
      except:
        continue
    conn.close()


def drop_summaries():
  conn = psycopg2.connect(DB_INFO)
  c = conn.cursor()

  c.execute('''DROP TABLE IF EXISTS summaries;''')
  conn.commit()


if __name__ == '__main__':
  print(sys.argv)
  if sys.argv[1] == "extract_qid":
    extract_qid(sys.argv[2])
  elif sys.argv[1] == "download_summary":
    download_summary(sys.argv[2])
  elif sys.argv[1] == "summary_for_qid":
    download_qid_summary()
  elif sys.argv[1] == "drop_summaries":
    drop_summaries()
  else:
    load_wikipedia_list(sys.argv[1])
