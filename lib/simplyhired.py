#! /usr/bin/env python

import glob
import json
import warc
from bs4 import BeautifulSoup
from warctools import parse_warc_payload
import itertools
import sys

IN_FILE = sys.argv[1]
OUT_FILE = sys.argv[2]

def read_simplyhired_data(filename):
  f = warc.open(filename)
  for record in f:
    # Ignore DNS records
    if record['content-type'] == 'text/dns':
      continue
    http_headers, content = parse_warc_payload(record.payload.read())
    soup = BeautifulSoup(content)
    if soup.select('div.detail .job_info'):
      # Get the job metadata and yield to iterator
      job_company = soup.select('div.detail .company')[0].text.strip().replace("Company: ", "")
      job_title = soup.select('div.detail .title')[0].text.strip()
      job_location = soup.select('div.detail .location')[0].text.strip().replace("Location: ", "")
      job_description = soup.select('div.detail .description_full')[0].get_text("\n", strip=True)
      # job_is_telecommute = soup.find_all("dt",text="Telecommute:")[0].find_next_sibling().text.strip()
      yield {
        "source": record.url,
        "company": job_company,
        "title": job_title,
        "location": job_location,
        "description": job_description
      }

print "Reading %s" % IN_FILE
jobs_iterator = read_simplyhired_data(IN_FILE)
with open(OUT_FILE,"w") as out:
  print "Writing processed data to %s" % OUT_FILE
  for job in jobs_iterator:
    out.write(json.dumps(job) + "\n")