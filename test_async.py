import getpass
import snowflake.connector 
import time

try:
    pwd_msg = "Please provide your Snowflake password: "
    snow_pwd = getpass.getpass(prompt = pwd_msg)
    con = snowflake.connector.connect(
        user = "username here", 
        password = snow_pwd, 
        account = "account here", 
        role = "role here", 
        warehouse = "wh here", 
        database = "db here", 
        schema = "schema here")

    execution_status = "initialize"

    sqlquery = "create or replace table test_acct_usage_q_hist as "
    sqlquery += " SELECT * FROM snowflake.account_usage.query_history order by start_time; "

    cur = con.cursor()
    cur.execute(sqlquery, _no_results=True)
    query_id = str(cur.sfqid)
    print(f"...submitted queryID: {query_id}")

    sqlquery = "SELECT execution_status FROM ( "
    sqlquery += " SELECT * "
    sqlquery += " FROM TABLE(information_schema.query_history_by_session())) "
    sqlquery += " WHERE query_id = '" + str(cur.sfqid) + "';"

    #loop max 100x so no runaways, 
    #  check the status and if <> RUNNING, break and exit
    n = 100
    while n > 0:
        time.sleep(5)
        n -= 1
        rs = cur.execute(sqlquery).fetchall()
        for rec in rs:
            execution_status = rec[0] 

        if execution_status  == "RUNNING" or execution_status  == "initialize":
            print(f"sleeping 5 seconds - current status is: {execution_status} - loopCount: {n}")
        else:
            break

    print('Loop ended.')
    print(f"...final status of query is: {execution_status}")

    con.close()

except Exception as e:
  print("An exception occurred: " + str(e))
  raise