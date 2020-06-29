# snowCD-lite-python.py
"""
Name:
    snowCD-lite-python.py 

Objectives:
    A poor mans SnowCD using Python

Problem:
    send up the bat signal or call the bat phone

"""

import json
import pprint
import logging
import requests
from requests_toolbelt.utils import dump

whitelist_json_filename = "whitelist.json"
logfile_filename = "snowCD-lite-python.log"

def read_whitelist_json():
    """
    read_whitelist_json reads the json document described in this link
    https://docs.snowflake.com/en/user-guide/snowcd.html

    Args:
       No inbound arguments

    Returns:
        a list of the contents of the json.

    Raises:
       if it fails it will fail big - go big or go home

    TODO:
       consider a modification should probably include a variable location for the file
       consider the ability to hard code contents of json in a variable

    """
    return_list = []

    with open(whitelist_json_filename) as f:
      data = json.load(f)

    for rec in data:
      rec_type = ""
      rec_host = ""
      rec_port = ""
      rec_url = ""

      for attribute, value in rec.items():
        if attribute == "type":
          rec_type = value
        if attribute == "host":
          rec_host = value
        if attribute == "port":
          rec_port = value

      if str(rec_port) == "443":
        rec_url = "https://" + rec_host
      else:
        rec_url = "http://" + rec_host

      if rec_host == "ocsp.snowflakecomputing.com":
        rec_url = str(rec_url) + "/ocsp_response_cache.json"

      return_list.append([rec_type, rec_host, rec_port, rec_url])

    return return_list

def main():
    """
    main is automatically executed when an process runs this script

    Args:
       None

    Returns:
        None

    Raises:
       if it fails it will fail big - go big or go home

    """
    print("Begin main() in snowCD-lite-python")

    logging.basicConfig(filename=logfile_filename, level=logging.INFO)
    requests_log = logging.getLogger("requests.packages.urllib3")
    requests_log.setLevel(logging.DEBUG)
    requests_log.propagate = True

    url_list = read_whitelist_json()

    output_list = []

    for rec in url_list:
      rec_type = rec[0]
      rec_host = rec[1]
      rec_port = rec[2]
      rec_url = rec[3]

      response = requests.get(rec_url)
      response_status_code = str(response.status_code)
      response_status_reason = response.reason

      if response_status_code == "200":
        msg = "msg: OK"
      elif response_status_code == "403":
        msg = "msg (see log file): 403 - FORBIDDEN - request understood, server refusing action"
      else:
        msg = "msg (see log file): " + response_status_code + " - OTHER"

      if response_status_code != "200":
        response_headers = str(response.headers)
        response_text = response.text
        response_dump = dump.dump_all(response)
        response_dump_str = response_dump.decode('utf-8')
        requests_log.error(" *** ERROR *** " + rec_host)
        requests_log.error(" " + response_status_code)
        requests_log.error(response_dump_str)
      elif rec_type in ["STAGE", "SNOWFLAKE_DEPLOYMENT"]:
        response_headers = str(response.headers)
        response_text = response.text
        response_dump = dump.dump_all(response)
        response_dump_str = response_dump.decode('utf-8')
        requests_log.info(" *** INFO *** STAGE *** " + rec_host)
        requests_log.info(" " + response_status_code)
        requests_log.info(response_dump_str)
      else:
        response_headers = "OK"
        response_text = "OK"
        response_dump_str = "OK"
 
      output_list.append(
        [rec_type, 
        rec_host, 
        rec_port, 
        rec_url, 
        response_status_code, 
        response_status_reason, 
        msg, 
        response_headers, 
        response_text])

    pprint.pprint(output_list)
    print("game over")


if __name__ == "__main__":
    main()

#eof


