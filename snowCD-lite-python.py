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
import requests
import pprint

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

    with open('whitelist.json') as f:
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
    url_list = read_whitelist_json()

    test_list = []

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
        msg = "msg: 403 - FORBIDDEN - request understood, server refusing action"
      else:
        msg = "msg: " + response_status_code + " - OTHER"

      if response_status_code != "200":
        response_headers = str(response.headers)
        response_text = response.text
      else:
        response_headers = "OK"
        response_text = "OK"

      test_list.append(
        [rec_type, 
        rec_host, 
        rec_port, 
        rec_url, 
        response_status_code, 
        response_status_reason, 
        msg, 
        response_headers, 
        response_text])

    pprint.pprint(test_list)
    print("game over")


if __name__ == "__main__":
    main()

#eof


