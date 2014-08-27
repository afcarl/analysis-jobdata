#! /usr/bin/env python

import glob
import json
import warc
from bs4 import BeautifulSoup
from warctools import parse_warc_payload
import itertools

OUT_FILE = "data/dice_processed/jobs.jsonlines"

def read_dice_data(filename):
  f = warc.open(filename)
  for record in f:
    # Ignore DNS records
    if record['content-type'] == 'text/dns':
      continue
    http_headers, content = parse_warc_payload(record.payload.read())
    soup = BeautifulSoup(content)
    if soup.select('meta[name="twitter:text:job_description_web"]'):
      # Get the job metadata and yield to iterator
      job_company = soup.select('meta[name="twitter:text:company"]')[0].attrs["content"].strip()
      job_title = soup.select('meta[name="twitter:text:job_title"]')[0].attrs["content"].strip()
      job_city = soup.select('meta[name="twitter:text:city"]')[0].attrs["content"].strip()
      job_state = soup.select('meta[name="twitter:text:state"]')[0].attrs["content"].strip()
      job_description = soup.select('meta[name="twitter:text:job_description_web"]')[0].attrs["content"].strip()
      # job_is_telecommute = soup.find_all("dt",text="Telecommute:")[0].find_next_sibling().text.strip()
      yield {
        "company": job_company,
        "title": job_title,
        "location": "%s, %s" % (job_city, job_state),
        "description": job_description
      }

print "Writing processed data to %s" % OUT_FILE
with open(OUT_FILE,"w") as out:
  for warc_file in glob.glob("data/dice/*.warc"):
    print "Processing %s" % warc_file
    jobs_iterator = read_dice_data(warc_file)
    for job in jobs_iterator:
      out.write(json.dumps(job) + "\n")