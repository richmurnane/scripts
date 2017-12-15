# bqTools.py
# sudo pip install --upgrade google-api-python-client
# sudo pip install --upgrade google-cloud-bigquery
# sudo pip install --upgrade google-datalab-bigquery
# sudo pip install --upgrade google-cloud-storage
# Please set GOOGLE_APPLICATION_CREDENTIALS or explicitly
# create credential and re-run the application.
# For more information, please see
# https://developers.google.com/accounts/docs/application-default-credentials
# Note:  if project arguement is left to None in the below, your default project will be used

from pandas.io import gbq
import json
import re
import google.auth
from subprocess import call
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.cloud.bigquery import LoadJobConfig
from google.cloud.bigquery import Table
from google.cloud import storage
import pandas as pd
import datetime

# ------------------------------------------------------------------
# project functions
# ------------------------------------------------------------------

def print_project_names():
    "print_project_names prints a list of projects your account has access to stdout"
    bigquery_client = bigquery.Client()
    for project in bigquery_client.list_projects():
        print(project.project_id)


# ------------------------------------------------------------------
# dataset functions
# ------------------------------------------------------------------

def dataset_exists(dataset_name, project=None):
    "dataset_exists returns a True/False, depending on if the bq dataset exists in the project"
    try:
        bigquery_client = bigquery.Client(project=project)
        dataset_ref = bigquery_client.dataset(dataset_name)
        bigquery_client.get_dataset(dataset_ref)
        return True
    except NotFound:
        return False

def create_dataset(dataset_name, project=None):
    "create_dataset creates a bq dataset, if already exists it will tell you"
    try:

        bigquery_client = bigquery.Client(project=project)
        if dataset_exists(dataset_name, project) is True:
            returnMsg = 'Existing dataset {}.'.format(dataset_name)
            return returnMsg

        dataset_ref = bigquery_client.dataset(dataset_name)
        bigquery_client.create_dataset(bigquery.Dataset(dataset_ref))

        output_dict = {
            "dataset_name": dataset_name,
            "status": "complete",
            "msg": 'Created dataset {}.'.format(dataset_name)
        }

        return output_dict

    except Exception as e:
        errorStr = 'ERROR (create_dataset): ' + str(e)
        print(errorStr)
        raise

def print_dataset_names(project=None):
    """print_dataset_names prints to stdout all datasets in a given project.
    If no project is specified, then the currently active project is used.
    """
    bigquery_client = bigquery.Client(project=project)
    for dataset in bigquery_client.list_datasets():
        print(dataset.dataset_id)

def delete_dataset(dataset_name, project=None):
    """delete_dataset will drop a dataset.
    Note:  I haven't tried to drop one with tables in it, #todo test that"
    """
    try:
        bigquery_client = bigquery.Client(project=project)
        dataset_ref = bigquery_client.dataset(dataset_name)
        if dataset_exists(dataset_name, project=project):
            dataset_ref = bigquery_client.get_dataset(dataset_ref)
            bigquery_client.delete_dataset(dataset_ref)

        output_dict = {
            "dataset_name": dataset_name,
            "status": "complete",
            "msg": 'Delete dataset {}.'.format(dataset_name)
        }

        return output_dict

    except Exception as e:
        errorStr = 'ERROR (delete_dataset): ' + str(e)
        print(errorStr)
        raise


# ------------------------------------------------------------------
# table functions
# ------------------------------------------------------------------

def print_table_names(dataset_name, project=None):
    try:
        """print_table_names prints to stdout all of the tables in a given dataset.
        If no project is specified, then the currently active project is used.
        """
        bigquery_client = bigquery.Client(project=project)
        dataset_ref = bigquery_client.dataset(dataset_name)

        for table in bigquery_client.list_dataset_tables(dataset_ref):
            print(table.table_id)

        returnMsg = 'print_table_names dataset {}.'.format(dataset_name)

        return returnMsg

    except Exception as e:
        errorStr = 'ERROR (print_table_names): ' + str(e)
        print(errorStr)
        raise

def table_exists(dataset_name, table_name, project=None):
    "table_exists returns a True/False - does the table live in this dataset?"
    try:
        bigquery_client = bigquery.Client(project=project)
        dataset_ref = bigquery_client.dataset(dataset_name)

        for table in bigquery_client.list_dataset_tables(dataset_ref):
            if table.table_id == table_name:
                return True

        return False

    except Exception as e:
        errorStr = 'ERROR (table_exists): ' + str(e)
        print(errorStr)
        raise

def create_empty_table(dataset_name, table_name, schema=None, project=None):
    """create_empty_table creates an empty table with the provided schema, 
    note the schema format should be the same as the bq cli.
    if no schema provided, it uses a dummy/sample schema"""
    try:
        bigquery_client = bigquery.Client(project=project)
        if not schema:
            schema = """[
                     {
                       "description": "col1",
                       "name": "col1",
                       "type": "STRING",
                       "mode": "NULLABLE"
                     }
                    ]
                    """

        schemaList = convert_schema(schema)

        if table_exists(dataset_name, table_name, project) is True:
            returnMsg = 'ERROR (create_empty_table) Existing Table: {}.'.format(dataset_name)
            return returnMsg

        dataset_ref = bigquery_client.dataset(dataset_name)
        table_ref = dataset_ref.table(table_name)
        # table_ref.schema = schema
        table = Table(table_ref, schema=schemaList)
        table = bigquery_client.create_table(table)

        output_dict = {
            "dataset_name": dataset_name,
            "table_name": table_name,
            "status": "complete",
            "msg": 'Created table {}.'.format(table_name)
        }

        return output_dict

    except Exception as e:
        errorStr = 'ERROR (create_empty_table): ' + str(e)
        print(errorStr)
        raise

def create_table_as_select(dataset_name, table_name, sqlQuery, project=None):
    """create_table_as_select - classic Create Table As Select (CTAS), uses CREATE_IF_NEEDED and WRITE_APPEND,
    these two options could be added later as arguments if needed"""
    try:
        bigquery_client = bigquery.Client(project=project)
        job_config = bigquery.QueryJobConfig()

        # Set configuration.query.destinationTable
        dataset_ref = bigquery_client.dataset(dataset_name)
        table_ref = dataset_ref.table(table_name)

        job_config.destination = table_ref

        # Set configuration.query.createDisposition
        job_config.create_disposition = 'CREATE_IF_NEEDED'

        # Set configuration.query.writeDisposition
        job_config.write_disposition = 'WRITE_APPEND'

        # Start the query
        job = bigquery_client.query(sqlQuery, job_config=job_config)

        # Wait for the query to finish
        job.result()

        output_dict = {
            "dataset_name": dataset_name,
            "table_name": table_name,
            "sqlQuery": sqlQuery,
            "job_id": job.job_id,
            "status": "complete",
            "msg": 'Created table {} .'.format(table_name)
        }

        return output_dict

    except Exception as e:
        errorStr = 'ERROR (create_table_as_select): ' + str(e)
        print(errorStr)
        raise

def create_table_as_select_cli(dataset_name, table_name, sqlQuery, project=None):
    try:
        """create_table_as_select_cli - using google command line interface CLI 
        I do not anticipate using this one very often since we have create_table_as_select(),
        leaving it in the code for reference though.
        Note:  head -1 is there because the CLI prints out to stdout the record, blah
        shell=true because you need to inherit GOOGLE_APPLICATION_CREDENTIALS
        """
        osCmd = "bq query --use_legacy_sql=false --destination_table '" + dataset_name + "." + table_name + "' --allow_large_results \"" + sqlQuery + " \" | head -1"
        print("begin: " + str(osCmd))
        resultCode = call(osCmd, shell=True)
        returnMsg = 'Created table: {} result code: {}.'.format(table_name, resultCode)

        output_dict = {
            "dataset_name": dataset_name,
            "table_name": table_name,
            "sqlQuery": sqlQuery,
            "job_id": job.job_id,
            "status": "complete",
            "msg": returnMsg
        }

        return output_dict

    except Exception as e:
        errorStr = 'ERROR (create_table_as_select_cli): ' + str(e)
        print(errorStr)
        raise

def copy_table(dataset_name, source_table_name, dest_table_name, project=None):
    "copy_table clones the source table to the destination table within the same dataset"
    try:

        bigquery_client = bigquery.Client(project=project)
        if table_exists(dataset_name, dest_table_name, project) is True:
            returnMsg = 'ERROR (copy_table) Existing Table: {}.'.format(dataset_name)
            return returnMsg

        dataset_ref = bigquery_client.dataset(dataset_name)
        source_table_ref = dataset_ref.table(source_table_name)
        dest_table_ref = dataset_ref.table(dest_table_name)
        job_config = bigquery.CopyJobConfig()
        copy_job = bigquery_client.copy_table(source_table_ref, dest_table_ref, job_config=job_config)
        copy_job.result()  # Waits for job to complete.

        output_dict = {
            "dataset_name": dataset_name,
            "source_table_name": source_table_name,
            "dest_table_name": dest_table_name,
            "job_id": copy_job.job_id,
            "status": "complete",
            "msg": 'copy_table: Table {} copied to {}.'.format(source_table_name, dest_table_name)
        }

        return output_dict

    except Exception as e:
        errorStr = 'ERROR (copy_table): ' + str(e)
        print(errorStr)
        raise

def drop_table(dataset_name, table_name, project=None):
    """drop_table - drops the table, whamo - good luck
    Deletes a table in a given dataset.
    If no project is specified, then the currently active project is used.
    """
    try:
        bigquery_client = bigquery.Client(project=project)
        dataset_ref = bigquery_client.dataset(dataset_name)
        table_ref = dataset_ref.table(table_name)

        if table_exists(dataset_name, table_name):
            bigquery_client.delete_table(table_ref)
            myStatus = "complete/dropped"
        else:
            myStatus = "complete/table-not-exists"

        output_dict = {
            "dataset_name": dataset_name,
            "table_name": table_name,
            "status": myStatus,
            "msg": 'Table {}:{} delete command complete.'.format(dataset_name, table_name)
        }

        return output_dict

    except Exception as e:
        errorStr = 'ERROR (delete_table): ' + str(e)
        print(errorStr)
        raise

def print_table_meta(dataset_name, table_name, project=None):
    "print_table_meta prints to stdout the num of rows (via gcp meta), schema, and description"
    try:
        bigquery_client = bigquery.Client(project=project)
        dataset = bigquery_client.dataset(dataset_name)
        table_ref = dataset.table(table_name)
        table = bigquery_client.get_table(table_ref)
        print(table.table_id)
        print("table.table_id: " + str(table.table_id))
        print("table.description: " + str(table.description))
        print("table.num_rows: " + str(table.num_rows))
        print("table.schema: " + str(table.schema))

        returnMsg = 'print_table_meta table: {}.'.format(table_name)
        output_dict = {
            "dataset_name": dataset_name,
            "table_name": table_name,
            "status": "complete",
            "msg": returnMsg
        }

        return output_dict

    except Exception as e:
        errorStr = 'ERROR (print_table_meta): ' + str(e)
        print(errorStr)
        raise

def rename_table(dataset_name, table_name, new_table_name, project=None):
    """rename_table renames a table within the same dataset,
    to do this it copies and then removes the original, not pretty but there is no 
    rename function in bq at this time"""
    try:
        if table_exists(dataset_name, new_table_name, project) is True:
            returnMsg = 'ERROR (rename_table) new_table_name exists: {}.'.format(new_table_name)
            return returnMsg

        if table_exists(dataset_name, table_name, project) is False:
            returnMsg = 'ERROR (rename_table) table_name does not exist: {}.'.format(table_name)
            return returnMsg

        copyResult = copy_table(dataset_name, table_name, new_table_name, project)
        dropResult = drop_table(dataset_name, table_name, project)

        output_dict = copyResult

        output_dict.update({"rename_table":"rename_table"})
        output_dict.update({"rename_status":"complete"})
        
        return output_dict

    except Exception as e:
        errorStr = 'ERROR (copy_table): ' + str(e)
        print(errorStr)
        raise

def get_table_schema(dataset_name, table_name, project=None):
    "get_table_schema returns an object containing the schema information for a table"
    try:
        bigquery_client = bigquery.Client(project=project)
        dataset = bigquery_client.dataset(dataset_name)
        table_ref = dataset.table(table_name)
        table = bigquery_client.get_table(table_ref)
        return table.schema

    except Exception as e:
        errorStr = 'ERROR (print_table_schema): ' + str(e)
        print(errorStr)
        raise

def get_table_columns_df(dataset_name, table_name, project=None):
    "get_table_columns_df returns a pandas dataframe containing the field names for a table"
    try:
        bigquery_client = bigquery.Client(project=project)
        dataset = bigquery_client.dataset(dataset_name)
        table_ref = dataset.table(table_name)
        table = bigquery_client.get_table(table_ref)
        field_dict = {}
        myCount = 0
        for field in table.schema:
            myCount = myCount + 1
            field_dict.update({myCount:field.name})

        df = pd.DataFrame.from_dict(field_dict, 'index')
        return df

    except Exception as e:
        errorStr = 'ERROR (get_table_columns): ' + str(e)
        print(errorStr)
        raise

# ------------------------------------------------------------------
# query functions
# ------------------------------------------------------------------

def print_25_rows(dataset_name, table_name, project=None):
    """print_25_rows prints rows to standard output in the given table.
    Will print 25 rows at most for brevity as tables can contain large amounts of rows. 
    If no project is specified, then the currently active project is used.
    """
    try:
        bigquery_client = bigquery.Client(project=project)
        dataset_ref = bigquery_client.dataset(dataset_name)
        table_ref = dataset_ref.table(table_name)

        # Get the table from the API so that the schema is available.
        table = bigquery_client.get_table(table_ref)

        # Load at most 25 results.
        rows = bigquery_client.list_rows(table, max_results=25)

        # Use format to create a simple table.
        format_string = '{!s:<16} ' * len(table.schema)

        # Print schema field names
        field_names = [field.name for field in table.schema]
        print(format_string.format(*field_names))

        for row in rows:
            print(format_string.format(*row))

        returnMsg = 'print_25_rows table {} in dataset {}.'.format(table_name, dataset_name)
        output_dict = {
            "dataset_name": dataset_name,
            "table_name": table_name,
            "status": "complete",
            "msg": returnMsg
        }

        return output_dict

    except Exception as e:
        errorStr = 'ERROR (print_25_rows): ' + str(e)
        print(errorStr)
        raise

def get_dataframe(sqlQuery, project=None, index_col=None, col_order=None, reauth=False, verbose=False, private_key=None, dialect='legacy'):
    "get_dataframe returns a pandas dataframe for a query, nice!"
    try:
        # the read_gbq requires the project_id(project name), so fetch it if none passed in
        if not project:
            credentials, project_id = google.auth.default(scopes=['https://www.googleapis.com/auth/iam'])
            project = str(project_id)

        df_gbq = gbq.read_gbq(sqlQuery, project, index_col, col_order, reauth, verbose, private_key, dialect)

        return df_gbq

    except Exception as e:
        errorStr = 'ERROR (get_dataframe): ' + str(e)
        print(errorStr)
        raise

def query_standard_sql(sqlQuery, print_stdout=True):
    """ query_standard_sql allows you to just fire/forget a query to bq, Standard SQL
        allows you to turn on/off stdout for results, and returns a message back to you
        SQL should include fully qualified table names: `bigquery-public-data.samples.wikipedia`
    """
    job_config = bigquery.QueryJobConfig()

    # Set use_legacy_sql to False to use standard SQL syntax.
    # Note that queries are treated as standard SQL by default.
    job_config.use_legacy_sql = False
    query_job = bigquery_client.query(sqlQuery, job_config=job_config)

    # Print the results.
    if print_stdout:
        for row in query_job.result():  # Waits for job to complete.
            print(row)
    else:
        query_job.result()  # Waits for job to complete.

    output_dict = {
        "sqlQuery": sqlQuery,
        "status": "complete",
        "job_id": query_job.job_id,
        "msg": 'query_standard_sql: Complete {}'.format(sql)
    }

    return output_dict

def print_1_rows(dataset_name, table_name, project=None):
    try:
        """print_1_rows - created by Bandhav - Prints rows in the given table.
        Will print 25 rows at most for brevity as tables can contain large amounts
        of rows. If no project is specified, then the currently active project is used.
        """
        bigquery_client = bigquery.Client(project=project)
        dataset_ref = bigquery_client.dataset(dataset_name)
        table_ref = dataset_ref.table(table_name)

        # Get the table from the API so that the schema is available.
        table = bigquery_client.get_table(table_ref)

        # Load at most 1 results.
        rows = bigquery_client.list_rows(table, max_results=1)

        # Use format to create a simple table.
        format_string = '{!s:<16} ' * len(table.schema)
        print(format_string)

        # Print schema field names
        field_names = [field.name for field in table.schema]
        # print(format_string.format(*field_names))
        print(field_names)

        #for row in rows:
         #   print(format_string.format(*row))

       # returnMsg = 'print_1_rows table {} in dataset {}.'.format(table_name, dataset_name)

        return field_names

    except Exception as e:
        errorStr = 'ERROR (print_1_rows): ' + str(e)
        print(errorStr)
        raise


# ------------------------------------------------------------------
# import functions
# ------------------------------------------------------------------

def load_data_from_gcs_simple(dataset_name, table_name, source, max_bad_records=0, project=None):
    """
    load_data_from_gcs_simple loads a *NEW* table to bq from gcs without the schema, autodetect it - nice!
    """
    try:
        bigquery_client = bigquery.Client(project=project)
        dataset_ref = bigquery_client.dataset(dataset_name)
        table_ref = dataset_ref.table(table_name)
        job_config = bigquery.LoadJobConfig()
        job_config.create_disposition = 'CREATE_IF_NEEDED'
        job_config.skip_leading_rows = 1
        job_config.source_format = 'CSV'
        job_config.write_disposition = 'WRITE_TRUNCATE'
        job_config.autodetect = True
        if max_bad_records:
            job_config.max_bad_records = max_bad_records

        load_job = bigquery_client.load_table_from_uri(source, table_ref, job_config=job_config)

        load_job.result()  # Waits for job to complete

        print('Loaded {} rows into {}:{}.'.format(
            load_job.output_rows, dataset_name, table_name))

        job_id = load_job.job_id
        job_exception = load_job.exception
        job_state = load_job.state
        error_result = load_job.error_result
        job_statistics = load_job._job_statistics()
        badRecords = job_statistics['badRecords']
        outputRows = job_statistics['outputRows']
        inputFiles = job_statistics['inputFiles']
        inputFileBytes = job_statistics['inputFileBytes']
        outputBytes = job_statistics['outputBytes']

        output_dict = {
            "dataset_name": dataset_name,
            "table_name": table_name,
            "source": source,
            "job_id": job_id,
            "job_state": str(job_state),
            "error_result": str(error_result),
            "badRecords": str(badRecords),
            "outputRows": str(outputRows),
            "inputFiles": str(inputFiles),
            "inputFileBytes": str(inputFileBytes),
            "outputBytes": str(outputBytes),
            "status": "complete",
            "msg": 'load_data_from_gcs_simple: Table {}.'.format(table_name)
        }

        return output_dict

    except Exception as e:
        errorStr = 'ERROR (load_data_from_gcs_simple): ' + str(e)
        print(errorStr)
        raise

def load_table_from_gcs(dataset_name, table_name, schema, source, skip_leading_rows=1, source_format='CSV', max_bad_records=0, write_disposition='WRITE_EMPTY', field_delimiter=",", project=None):
    "load_table_from_gcs loads a *NEW* table to bq from gcs with the schema"
    try:

        bigquery_client = bigquery.Client(project=project)

        # convert the schema json string to a list
        schemaList = convert_schema(schema)

        dataset_ref = bigquery_client.dataset(dataset_name)
        table_ref = dataset_ref.table(table_name)
        table = bigquery.Table(table_ref, schema=schemaList)

        bigquery_client.create_table(table)

        job_id_prefix = "bqTools_load_job"
        job_config = bigquery.LoadJobConfig()
        job_config.create_disposition = 'NEVER'
        job_config.skip_leading_rows = skip_leading_rows
        job_config.source_format = source_format
        job_config.write_disposition = write_disposition

        if max_bad_records:
            job_config.max_bad_records = max_bad_records

        if field_delimiter:
            job_config.field_delimiter = field_delimiter

        load_job = bigquery_client.load_table_from_uri(
            source, table_ref, job_config=job_config,
            job_id_prefix=job_id_prefix)

        # the following waits for table load to complete
        load_job.result()

        print("------ load_job\n")
        print("load_job: " + str(type(load_job)))
        print(dir(load_job))

        print("------ load_job.result\n")
        job_result = load_job.result
        print("job_result: " + str(type(job_result)))
        print(job_result)

        job_exception = load_job.exception
        job_id = load_job.job_id
        job_state = load_job.state
        error_result = load_job.error_result
        job_statistics = load_job._job_statistics()
        badRecords = job_statistics['badRecords']
        outputRows = job_statistics['outputRows']
        inputFiles = job_statistics['inputFiles']
        inputFileBytes = job_statistics['inputFileBytes']
        outputBytes = job_statistics['outputBytes']

        print("\n ***************************** ")
        print(" job_state:      " + str(job_state))
        print(" error_result:   " + str(error_result))
        print(" job_id:         " + str(job_id))
        print(" badRecords:     " + str(badRecords))
        print(" outputRows:     " + str(outputRows))
        print(" inputFiles:     " + str(inputFiles))
        print(" inputFileBytes: " + str(inputFileBytes))
        print(" outputBytes:    " + str(outputBytes))
        print(" type(job_exception):  " + str(type(job_exception)))
        print(" job_exception:  " + str(job_exception))
        print(" ***************************** ")

        print("------ load_job.errors \n")
        myErrors = load_job.errors if load_job.errors is not None else []
        print("load_job.errors - count is : " + str(len(myErrors)))
        for errorRecord in myErrors:
            print(errorRecord)

        print("------ ------ ------ ------\n")

        # TODO:  need to figure out how to get # records failed, and which ones they are
        # research shoed "statistics.load_job" - but not sure how that works

        output_dict = {
            "dataset_name": dataset_name,
            "table_name": table_name,
            "source": source,
            "job_id": job_id,
            "job_state": str(job_state),
            "error_result": str(error_result),
            "badRecords": str(badRecords),
            "outputRows": str(outputRows),
            "inputFiles": str(inputFiles),
            "inputFileBytes": str(inputFileBytes),
            "outputBytes": str(outputBytes),
            "error_list": myErrors,
            "status": "complete",
            "msg": 'load_table_from_gcs {}:{} {}'.format(dataset_name, table_name, source)

        }

        return output_dict

    except Exception as e:
        errorStr = 'ERROR (load_table_from_gcs): ' + str(e)
        print(errorStr)
        raise

def load_table_from_csv(dataset_name, table_name, schema, local_file_name, skip_leading_rows=1, source_format='CSV', project=None):
    "load_table_from_csv loads a local file to bq"
    try:
        bigquery_client = bigquery.Client(project=project)

        # convert the schema json string to a list
        schemaList = convert_schema(schema)

        # load a local file
        dataset_ref = bigquery_client.dataset(dataset_name)

        table_ref = dataset_ref.table(table_name)

        job_config = LoadJobConfig()
        job_config.skip_leading_rows = skip_leading_rows
        job_config.source_format = source_format
        job_config.schema = schemaList

        with open(local_file_name, 'rb') as readable:
            # API request
            bigquery_client.load_table_from_file(readable, table_ref, job_config=job_config)

        output_dict = {
            "dataset_name": dataset_name,
            "table_name": table_name,
            "local_file_name": local_file_name,
            "status": "complete",
            "msg": 'load_table_from_csv {}:{} {}'.format(dataset_name, table_name, local_file_name)
        }

        return output_dict

    except Exception as e:
        errorStr = 'ERROR (load_table_from_csv): ' + str(e)
        print(errorStr)
        raise

def load_table_from_df(dataset_name, table_name, dataframe, project=None, chunksize=10000, verbose=False, reauth=False, if_exists='replace', private_key=None):
    "load_table_from_df loads a table from a pandas dataframe"
    try:
        # the read_gbq requires the project_id(project name), so fetch it if none passed in
        if not project:
            credentials, project_id = google.auth.default(scopes=['https://www.googleapis.com/auth/iam'])
            project = str(project_id)

        # Name of table to be written, in the form dataset.tablename
        destination_table = str(dataset_name) + "." + str(table_name)

        # https://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.to_gbq.html
        myResult = dataframe.to_gbq(destination_table, project, chunksize=chunksize, verbose=verbose, reauth=reauth, if_exists=if_exists, private_key=private_key)

        output_dict = {
            "dataset_name": str(dataset_name),
            "table_name": str(table_name),
            "destination_table": str(destination_table),
            "project": str(project),
            "myResult": str(myResult),
            "status": "complete",
            "msg": 'load_table_from_df {}'.format(destination_table)
        }

        return output_dict

    except Exception as e:
        errorStr = 'ERROR (load_table_from_df): ' + str(e)
        print(errorStr)
        raise

def load_table_from_gcs_fixedwidth(dataset_name, table_name, fixedwidth_spec, source, skip_leading_rows=1, max_bad_records=0, project=None):
    """  load_table_from_gcs_fixedwidth loads a fixed width format file from gcs to 
    bq with fixedwidth_spec
        Note on fixedwidth_spec: a string that follows the below format:
                ColumnName1:Width:DataType,ColumnName2:Width:DataType,...

            Width is the field's width and must be an integer
            DataType is the field's data type and must be either STRING, INTEGER, FLOAT, BOOLEAN, or TIMESTAMP
        example fixedwidth_spec= "Id:3:INTEGER,firstName:9:STRING,lastName:10:STRING,ISBN:11:STRING"
    """
    temp_schema = """[
                            {
                               "description": "full record",
                               "name": "fullstring",
                               "type": "STRING",
                               "mode": "NULLABLE"
                             }
                            ]
                            """
    temp_table_name = table_name + '_tmp'
    try:
        _ = drop_table(dataset_name, temp_table_name)
        print str(_["msg"])

        # load fixed width files into a temp table of one full string column
        load_job_output = load_table_from_gcs(dataset_name, temp_table_name, temp_schema, source, skip_leading_rows, 'CSV', max_bad_records, 'WRITE_EMPTY', ",", project)
        print str(load_job_output['msg'])
        # get the select query from the provided fixedwidth_spec
        sqlQuery = convert_sqlquery_from_fixedwidth_spec(dataset_name, temp_table_name, fixedwidth_spec, full_col_name="fullstring")
        print 'sqlQuery: ', sqlQuery

        _ = drop_table(dataset_name, table_name)
        print str(_["msg"])

        # create the final table from select SQL query
        select_job_output = create_table_as_select(dataset_name, table_name, sqlQuery, project)
        print select_job_output['msg']

        # finally, drop temp table
        _ = drop_table(dataset_name, temp_table_name, project)
        print str(_["msg"])

        output_dict = {
            "dataset_name": dataset_name,
            "table_name": table_name,
            "source": source,
            "load_from_gcs_job_id": load_job_output['job_id'],
            "select_query_job_id": select_job_output['job_id'],
            "error_list": load_job_output['error_list'],
            "sqlQuery": sqlQuery,
            "error_result": load_job_output['error_result'],
            "badRecords":  load_job_output['badRecords'],
            "outputRows": load_job_output['outputRows'],
            "inputFiles": load_job_output['inputFiles'],
            "inputFileBytes": load_job_output['inputFileBytes'],
            "outputBytes": load_job_output['outputBytes'],
            "status": "complete",
            "msg": 'load_table_from_gcs_fixedwidth {}:{} {}'.format(dataset_name, table_name, source)
        }

        return output_dict

    except Exception as e:
        errorStr = 'ERROR (load_table_from_gcs_fixedwidth): ' + str(e)
        print(errorStr)
        raise


# ------------------------------------------------------------------
# export functions
# ------------------------------------------------------------------

def export_table_to_gcs(dataset_name, table_name, destination, field_delimiter=",", print_header=None, destination_format="CSV", compression="GZIP", project=None):
    """export_table_to_gcs exports a table from bq into a file on gcs,
        the destination should look like the following, with no brackets {}
        gs://{bucket-name-here}/{file-name-here}
        python export_data_to_gcs.py example_dataset example_table gs://example-bucket/example-data.csv
        The dataset and table should already exist
        py> myMsg = bqTools.export_data_to_gcs('rmurnane_dev01', 'dummy_data13',
                        'gs://merkle-cloud-innov-01-gcp/fake-dummy_data13_201711211526.csv',
                        field_delimiter="|", print_header=True, destination_format="CSV",
                        compression="GZIP")
    """
    try:
        bigquery_client = bigquery.Client(project=project)
        dataset_ref = bigquery_client.dataset(dataset_name)
        table_ref = dataset_ref.table(table_name)

        job_id_prefix = "bqTools_export_job"

        job_config = bigquery.ExtractJobConfig()

        # default is ","
        if field_delimiter:
            job_config.field_delimiter = field_delimiter

        # default is true
        if print_header:
            job_config.print_header = print_header

        # CSV, NEWLINE_DELIMITED_JSON, or AVRO
        if destination_format:
            job_config.destination_format = destination_format

        # GZIP or NONE
        if compression:
            job_config.compression = compression

        # if it should be compressed, make sure there is a .gz on the filename, add if needed
        if compression == "GZIP":
            if destination.lower()[-3:] != ".gz":
                destination = str(destination) + ".gz"

        job = bigquery_client.extract_table(table_ref, destination, job_config=job_config, job_id_prefix=job_id_prefix)

        # job.begin()
        job.result()  # Wait for job to complete

        output_dict = {
            "dataset_name": dataset_name,
            "table_name": table_name,
            "destination": destination,
            "job_id": job.job_id,
            "status": "complete",
            "msg": 'Exported {}:{} to {}'.format(dataset_name, table_name, destination)
        }

        return output_dict

    except Exception as e:
        errorStr = 'ERROR (export_data_to_gcs): ' + str(e)
        print(errorStr)
        raise

def export_query_to_gcs(dataset_name, sqlQuery, destination, field_delimiter=",", print_header=None, destination_format="CSV", compression="GZIP", keep_temp_table=None, project=None):
    """export_query_to_gcs creates a temporary table and export it to gs, then drop the temp table """
    try:

        my_date = datetime.datetime.now()
        date_str = str(my_date.year) + str(my_date.month) + str(my_date.day) + str(my_date.hour) + str(my_date.minute) + str(my_date.second) + '0000'
        tmp_table_name =  "TMP_EXPORT_" + date_str.upper()

        if table_exists(dataset_name, tmp_table_name):
            myResult = drop_table(dataset_name, tmp_table_name)

        # comment out print if not needed
        print("creating TMP table " + str(tmp_table_name))
        tmpTableResult = create_table_as_select(dataset_name, tmp_table_name, sqlQuery)
        # comment out print if not needed
        print(tmpTableResult)

        print("begin export " + str(tmp_table_name))
        exportTableResult = export_table_to_gcs(
                                dataset_name, tmp_table_name, destination, 
                                field_delimiter=field_delimiter, print_header=print_header, 
                                destination_format=destination_format, compression=compression, 
                                project=project)
        # comment out print if not needed
        # print(exportTableResult)

        output_dict = exportTableResult

        if keep_temp_table is None:
            keep_temp_table = "NO"

        if keep_temp_table.upper() == "YES":
            print("temporary table not dropped")
            output_dict.update({"tmp_table_kept":"YES"})
        else:
            if table_exists(dataset_name, tmp_table_name):
                myResult = drop_table(dataset_name, tmp_table_name)
                output_dict.update({"tmp_table_kept":"NO"})
            else:
                output_dict.update({"tmp_table_kept":"N/A"})
        
        return output_dict

    except Exception as e:
        errorStr = 'ERROR (export_query_to_gcs): ' + str(e)
        print(errorStr)
        raise


# ------------------------------------------------------------------
# view functions
# ------------------------------------------------------------------

# note:  in bq, you do not use DDL - "create view as select ...."
def create_view(dataset_name, view_name, sqlQuery, project=None):
    """
    create_view creates a view using the SQL passed in,
    note bq requires you to use standardSQL when querying a view created with standardSQL 
    and legacySQL when querying a view created with legacySQL
    Suggestion:  please try to use standardSQL if possible
    """
    try:

        bigquery_client = bigquery.Client(project=project)
        dataset_ref = bigquery_client.dataset(dataset_name)
        table_ref = dataset_ref.table(view_name)
        table = Table(table_ref)
        table.view_query = sqlQuery
        table.view_use_legacy_sql = False
        bigquery_client.create_table(table)

        return True

    except Exception as e:
        errorStr = 'ERROR (create_view): ' + str(e)
        print(errorStr)
        raise


# ------------------------------------------------------------------
# misc functions
# ------------------------------------------------------------------

def convert_schema(schema_json_str):
    "convert_schema converts a standard bq cli json string into list formatted for bq python"
    try:
        schema_str = schema_json_str.replace('\n', '')
        schema_str = re.sub('\s+', ' ', schema_str)
        schemaJson = json.loads(schema_str)

        SCHEMA = []
        for item in schemaJson:
            fieldName = item["name"]
            fieldType = item["type"]
            fieldMode = item["mode"]
            myThing = bigquery.SchemaField(fieldName.encode('utf-8'), fieldType.encode('utf-8'), mode=fieldMode.encode('utf-8'))
            SCHEMA.append(myThing)

        return SCHEMA

    except Exception as e:
        errorStr = 'ERROR (create_dataset): ' + str(e)
        print(errorStr)
        raise

def convert_sqlquery_from_fixedwidth_spec(dataset_name, table_name, fixedwidth_spec, full_col_name ='fullstring'):
    """  convert_sqlquery_from_fixedwidth_spec creates a select sql query from the fixedwidth_spec and the provided table specs
        Note on fixedwidth_spec: a string that follows the below format:
            ColumnName1:Width:DataType,ColumnName2:Width:DataType,...

        Width is the field's width and must be an integer
        DataType is the field's data type and must be either STRING, INTEGER, FLOAT, BOOLEAN, or TIMESTAMP
    example fixedwidth_spec= "Id:3:INTEGER,firstName:9:STRING,lastName:10:STRING,ISBN:11:STRING"
    """
    sqlQuery ="SELECT "
    col_list = fixedwidth_spec.split(",")
    loc = 1

    for col in col_list:
        col_spec = col.split(":")
        if len(col_spec) != 3:
            errorStr = 'ERROR (convert_sqlquery_from_fixedwidth_spec): fixedwidth_spec is missing arguments'
            print(errorStr)
            print str(col_spec)
            raise
        else:
            name = col_spec[0]
            width = col_spec[1]
            data_type = col_spec[2]

            if data_type not in ('STRING', 'INTEGER', 'FLOAT', 'BOOLEAN', 'TIMESTAMP'):
                errorStr = 'ERROR (convert_sqlquery_from_fixedwidth_spec): data type must be either STRING, INTEGER, FLOAT, BOOLEAN, TIMESTAMP or RECORD'
                print(errorStr)
                raise
            else:
                if data_type == 'INTEGER':
                    sqlQuery = sqlQuery + 'CAST(LTRIM(RTRIM(SUBSTR(' + full_col_name + ',' + str(loc) + ',' + str(width) + '))) AS INT64) ' + name + ', '
                elif data_type == 'FLOAT':
                    sqlQuery = sqlQuery + 'CAST(LTRIM(RTRIM(SUBSTR(' + full_col_name + ',' + str(loc) + ',' + str(width) + '))) AS FLOAT64) ' + name + ', '
                elif data_type == 'BOOLEAN':
                    sqlQuery = sqlQuery + 'CAST(LTRIM(RTRIM(SUBSTR(' + full_col_name + ',' + str(loc) + ',' + str(width) + '))) AS BOOL) ' + name + ', '
                elif data_type == 'TIMESTAMP':
                    sqlQuery = sqlQuery + 'CAST(LTRIM(RTRIM(SUBSTR(' + full_col_name + ',' + str(loc) + ',' + str(width) + '))) AS TIMESTAMP) ' + name + ', '
                else:
                    sqlQuery = sqlQuery + 'SUBSTR(' + full_col_name + ',' + str(loc) + ',' + str(width) + ') ' + name + ', '
                # update location for the next field
                loc = loc + int(width)

    sqlQuery = sqlQuery[:-2] + ' FROM `' + dataset_name + '.' + table_name + '`'
    return sqlQuery

def run_sql_cli(dataset_name, sqlQuery, project=None):
    """run_sql_cli - from Bandhav - using CLI because CTAS and DDL/SQL not yet available
    head -1 is there because the CLI prints out to stdout the record, blah
    shell=true because you need to inherit GOOGLE_APPLICATION_CREDENTIALS
    """
    try:
        resultCode=[]
        osCmd = "bq query --use_legacy_sql=false  \"" + sqlQuery + " \""
        print("begin: " + str(osCmd))
        resultCode = call(osCmd, shell=True)
        print(resultCode)

    except Exception as e:
        errorStr = 'ERROR (run_sql_cli): ' + str(e)
        print(errorStr)
        raise



