import warc
from bs4 import BeautifulSoup
from warctools import parse_warc_payload

def read_authenticjob_data(filename):
  f = warc.open(filename)
  for record in f:
    # Ignore DNS records
    if record['content-type'] == 'text/dns':
      continue
    http_headers, content = parse_warc_payload(record.payload.read())
    soup = BeautifulSoup(content)
    if soup.find(id="listing"):
      # Get the job metadata and yield to iterator
      job_company = soup.find("div", "company").h2.text.strip()
      role = soup.find("div","role")
      job_title = role.h1.text.strip()
      job_type = role.h4.text.strip()
      job_location = role.find(id="location").span.text.strip()
      job_description = role.find(id="description").text.strip()
      yield {
        "company": job_company,
        "title": job_title,
        "type": job_type,
        "location": job_location,
        "description": job_description
      }