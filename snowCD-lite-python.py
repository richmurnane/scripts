# snowCD-lite-python.py
"""
Name:
    snowCD-lite-python.py 

Objectives:
    A poor mans SnowCD using Python

Instructions:
    1. push script to host experiencing Snowflake connection issues w/ Python3
    2. ensure you have the following libraries pip installed
        requests
        requests_toolbelt
        json
        logging
        dnsPython
    3. save whitelist.json to folder with the script 
        whitelist.json docs at https://docs.snowflake.com/en/user-guide/snowcd.html
    4. run with command like "python3 snowCD-lite-python.py"
    5. review STDOUT and snowCD-lite-python.log

Problem with the script?
    send up the bat signal or call the bat phone

"""

import json
import pprint
import logging
import requests
from requests_toolbelt.utils import dump
import dns.resolver

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


def print_output(test_count, ok_count, fail_count, results_list):
  """
  print_output prints the output of the tests to standard output, similar to SnowCD

  Args:
    test_count:   number of tests performed
    ok_count:     successful test count
    fail_count:   number of tests that failed
    results_list: a list of the tests, the "non 200" ones will be printed

  Returns:
    the number of records in the results_list    

  Raises:
    if it fails it will fail big - go big or go home

  TODO:
    N/A

  """    

  if fail_count >= 1:
    print("==============================================")
    print("Details of FAILED checks")
    print("Please see logfile for further details")
    print("==============================================")
    for rec in results_list:
      if str(rec[9]) != "OK":
        print("==============================================")
        print("failed check")
        print("==============================================")
        print("Host:     " + str(rec[1]))
        print("Port:     " + str(rec[2]))
        print("Type:     " + str(rec[0]))
        print("URL:      " + str(rec[3]))
        print("Response: " + str(rec[4]))
        print("Response: " + str(rec[5]))
        print("Response: " + str(rec[6]))
        print("Response: " + str(rec[7]))
        print("Response: " + str(rec[8]))
        print("CNames:   " + str(rec[10]))
        print("Suggestion: Check your networking, proxy, vpc configuration, etc...")
        print("==============================================")
        print("")

  print("==============================================")
  print("SUMMARY")
  print("==============================================")
  print("Num. checks:         " + str(test_count))
  print("Num. OK checks:      " + str(ok_count))
  print("Num. FAILED checks:  " + str(fail_count))
  print("Please see logfile for further details")
  print("==============================================")


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
  requests_log.info("...begin...")

  url_list = read_whitelist_json()

  output_list = []

  check_count = 0
  ok_count = 0
  fail_count = 0
  for rec in url_list:
    check_count += 1
    rec_type = rec[0]
    rec_host = rec[1]
    rec_port = rec[2]
    rec_url = rec[3]

    response = requests.get(rec_url)
    response_status_code = str(response.status_code)
    response_status_reason = response.reason
    response_headers = str(response.headers)
    response_text = response.text
    response_dump = dump.dump_all(response)
    response_dump_str = response_dump.decode('utf-8')

    cnames = "N/A"
    cnames_list = []

    try:
      cnames_results = dns.resolver.query(rec_host, 'CNAME')
      for cnames_data in cnames_results:
          cnames_list.append(str(cnames_data))

      cnames = str(cnames_list)
    except:
      cnames = "EXCEPTION IN CNAMES"

    if response_status_code == "200":
      msg = "msg: OK"
    elif response_status_code == "403":
      msg = "msg (see log file): 403 - FORBIDDEN - request understood, server refusing action"
    else:
      msg = "msg (see log file): " + response_status_code + " - OTHER"

    snowcd_status = "OK"
    if (response_status_code == "403" and  rec_type == "STAGE" and  response_dump_str.find("AccessDenied") >= 1):
      requests_log.warning(" *** WARN ***** " + rec_host)
      requests_log.warning(" *** STAGE and AccessDenied ***** " + rec_host)
      requests_log.warning(" " + response_status_code)
      requests_log.warning(response_dump_str)
      requests_log.warning(" ^^^ WARN ^^^^^ " + rec_host)
      ok_count += 1
    elif (response_status_code == "403" and  rec_type == "OUT_OF_BAND_TELEMETRY" and   response_dump_str.find("Missing Authentication Token") >= 1):
      requests_log.warning(" *** WARN ***** " + rec_host)
      requests_log.warning(" *** snowCD does not fail this ***** " + str(response_status_code))
      requests_log.warning(" *** OUT_OF_BAND_TELEMETRY and Missing Token ***** " + rec_host)
      requests_log.warning(" " + response_status_code)
      requests_log.warning(response_dump_str)
      requests_log.warning(" ^^^ WARN ^^^^^ " + rec_host)
      ok_count += 1
    elif response_status_code != "200":
      requests_log.error(" *** ERROR *** " + rec_host)
      requests_log.error(" " + response_status_code)
      requests_log.error(response_dump_str)
      requests_log.error(" ^^^ ERROR ^^^ " + rec_host)
      snowcd_status = "FAIL"
      fail_count += 1
    elif rec_type in ["STAGE", "SNOWFLAKE_DEPLOYMENT"]:
      ok_count += 1
    else:
      response_headers = "OK"
      response_text = "OK"
      response_dump_str = "OK"
      ok_count += 1

    requests_log.info("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
    requests_log.info("Host:     " + str(rec[1]))
    requests_log.info("rec_type: " + str(rec_type))
    requests_log.info("snowcd_status: " + str(snowcd_status))
    requests_log.info("Response: " + response_status_code)
    requests_log.info("CNames:   " + cnames)
    requests_log.info("Response Dump:   " + response_dump_str)
    requests_log.info("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")

    output_list.append(
      [rec_type, 
      rec_host, 
      rec_port, 
      rec_url, 
      response_status_code, 
      response_status_reason, 
      msg, 
      response_headers, 
      response_text,
      snowcd_status,
      cnames_data])

  #pprint.pprint(output_list)
  print_output(check_count, ok_count, fail_count, output_list)

  requests_log.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
  requests_log.info("check_count: " + str(check_count))
  requests_log.info("ok_count:    " + str(ok_count))
  requests_log.info("fail_count:  " + str(fail_count))
  requests_log.info("...game over...")
  requests_log.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

  print("game over")


if __name__ == "__main__":
    main()

#eof


