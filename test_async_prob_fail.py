import getpass
import snowflake.connector 

try:
    pwd_msg = "Please provide your Snowflake password: "
    snow_pwd = getpass.getpass(prompt = pwd_msg)
    con = snowflake.connector.connect(
        user = "rmurnane", 
        password = snow_pwd, 
        account = "aws_cas1", 
        role = "SYSADMIN", 
        warehouse = "RMURNANE_WH", 
        database = "RMURNANE_DB", 
        schema = "RMURNANE_SCHEMA")

    sqlquery = "create or replace table test_acct_usage_q_hist as "
    sqlquery += " SELECT * FROM snowflake.account_usage.query_history order by start_time; "

    cur = con.cursor()
    cur.execute(sqlquery, _no_results=True)
    query_id = str(cur.sfqid)
    out_msg = "...submitted queryID: {}".format(query_id)
    print(out_msg)
    con.close()

    print("the above will probably fail because we closed the connection")


except Exception as e:
  print("An exception occurred: " + str(e))
  raise