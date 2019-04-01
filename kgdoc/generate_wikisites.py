import json
from urllib.request import urlopen
from pathlib import Path
import csv

url = "https://meta.wikimedia.org/w/api.php?action=sitematrix&smsiteprop=url|dbname|code|lang|sitename&smtype=language&smlangprop=site|code|localname&format=json"

def get_jsonparsed_data(url):
  """
  Receive the content of ``url``, parse it as JSON and return the object.

  Parameters
  ----------
  url : str

  Returns
  -------
  dict
  """
  response = urlopen(url)
  data = response.read().decode("utf-8")
  return json.loads(data)

def gen_siteitem(json_response):
  """a generator yields the new data structure."""
  for k, v in json_response.items():
    if not isinstance(v, dict): continue
    code = v['code']
    sites = v['site']
    localname = v['localname']
    for site in sites:
      url = site['url']
      dbname = site['dbname']
      code = site['code']
      lang = site['lang']
      name = localname + " " + site['sitename']
      yield {"name": name, "code": code, "url": url, "dbname": dbname, "code": code, "lang": lang}


if __name__ == "__main__":
  r = get_jsonparsed_data(url)['sitematrix']
  items = gen_siteitem(r)
  p = Path.cwd() / 'src/main/resources/generated' / 'wikisites.csv'
  with p.open('w') as f:
    fieldnames = ['name', 'code', 'url', 'dbname', 'code', 'lang']
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    for row in items:
      writer.writerow(row)
