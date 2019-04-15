import sys
import lazynlp
from collections import defaultdict

def load_wikipedia_list(filename):
  lazynlp.dedup_lines_from_new_file([filename], new_file, outfile)


def extract_qid(filename):
  available_sites = set(["en", "zh", "sv"])

  urls = defaultdict(list)
  with open(filename, 'r') as f:
    for l in f:

      try::
        q_id, url = l.rstrip().split(' ')
        site = url[8:].split('.')[0]
        if site in available_sites:
          urls[q_id].append(url)
      except:
        continue

  for qid, urls in urls.items():
    with open("download_urls/" + qid, "w") as f:
      for url in urls:
        f.write(url + "\n")

if __name__ == '__main__':
  if sys.argv[1] == "extract_qid":
    extract_qid(sys.argv[2])
  else:
    load_wikipedia_list(sys.argv[1])
