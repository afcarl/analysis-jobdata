# Takes the RAW WARC payload and returns a tuple of (headers, HTML)
def parseWARCpayload(str):
  headers, content = str.split("\r\n\r\n", 1)
  # We start from index 1 to ignore the HTTP response code
  headers_dict = dict(map(lambda h: tuple(h.split(": ", 1)), headers.split("\r\n")[1:]))
  return (headers_dict, content.strip())