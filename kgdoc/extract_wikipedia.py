import sys
import lazynlp
from collections import defaultdict
import traceback

def load_wikipedia_list(filename):
  lazynlp.dedup_lines_from_new_file([filename], new_file, outfile)


def extract_qid(filename):
  import sqlite3
  conn = sqlite3.connect('download_urls.db')
  c = conn.cursor()

  c.execute('''CREATE TABLE urls
              (qid text, site text, url text);''')
  c.execute('''CREATE INDEX urls_idx_on_qid ON urls(qid);''')
  c.execute('''CREATE INDEX urls_idx_on_qid_site ON urls(qid, site);''')
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

if __name__ == '__main__':
  print(sys.argv)
  if sys.argv[1] == "extract_qid":
    extract_qid(sys.argv[2])
  else:
    load_wikipedia_list(sys.argv[1])
