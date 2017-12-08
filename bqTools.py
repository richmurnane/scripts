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

# ------------------------------------------------------------------
# misc functions
# ------------------------------------------------------------------

def convert_schema(schema_json_str):
    "converts a standard bq cli json string into list formatted for bq python"
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
    "this function returns a True/False, depending on if the bq dataset exists in the project"
    try:
        bigquery_client = bigquery.Client(project=project)
        dataset_ref = bigquery_client.dataset(dataset_name)
        bigquery_client.get_dataset(dataset_ref)
        return True
    except NotFound:
        return False

def create_dataset(dataset_name, project=None):
    "create a bq dataset using this function, if already exists it will tell you"
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
    """Lists all datasets in a given project.
    If no project is specified, then the currently active project is used.
    """
    bigquery_client = bigquery.Client(project=project)
    for dataset in bigquery_client.list_datasets():
        print(dataset.dataset_id)

def delete_dataset(dataset_name, project=None):
    """this function will drop a dataset.
    I haven't tried to drop one with tables in it, #todo test that"
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
        """Lists all of the tables in a given dataset.
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
    "return a True/False does the table live in this dataset?"
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
    "create a table with the schema, note the schema format should be the same as the bq cli"
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
        """Note:  using CLI because CTAS and DDL/SQL not yet available
        head -1 is there because the CLI prints out to stdout the record, blah
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
    "clone the table within the same dataset"
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
    "drop the table, whamo"
    try:
        """Deletes a table in a given dataset.
        If no project is specified, then the currently active project is used.
        """
        bigquery_client = bigquery.Client(project=project)
        dataset_ref = bigquery_client.dataset(dataset_name)
        table_ref = dataset_ref.table(table_name)
        bigquery_client.delete_table(table_ref)

        output_dict = {
            "dataset_name": dataset_name,
            "table_name": table_name,
            "status": "complete",
            "msg": 'Table {}:{} deleted.'.format(dataset_name, table_name)
        }

        return output_dict

    except Exception as e:
        errorStr = 'ERROR (delete_table): ' + str(e)
        print(errorStr)
        raise

def print_table_meta(dataset_name, table_name, project=None):
    "print out num rows, schema, and description"
    try:
        bigquery_client = bigquery.Client(project=project)
        dataset = bigquery_client.dataset(dataset_name)
        tables = list(bigquery_client.list_dataset_tables(dataset))

        for table in tables:
            if table.table_id == table_name:
                print(table.table_id)
                print("this following are null, google bq bug#4439 https://github.com/GoogleCloudPlatform/google-cloud-python/pull/4439")
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


# ------------------------------------------------------------------
# query functions
# ------------------------------------------------------------------

def print_25_rows(dataset_name, table_name, project=None):
    try:
        """Prints rows in the given table.
        Will print 25 rows at most for brevity as tables can contain large amounts
        of rows. If no project is specified, then the currently active project is used.
        """
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
    "this function returns a pandas dataframe for a query, nice!"
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
    """ this function allows you to just fire/forget a query to bq, Standard SQL
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

#def get_dataframe2(sqlQuery):
    #    "this function returns a pandas dataframe for a query, using datalab libs - nice!"
    #    try:
    #        bq_query = bq.Query(sqlQuery)
    #        df = bq_query.execute(output_options=bq.QueryOutput.dataframe()).result()
    #        return df
    #    except Exception as e:
    #        errorStr = 'ERROR (get_dataframe2): ' + str(e)
    #        print(errorStr)
    #        raise

# ------------------------------------------------------------------
# import functions
# ------------------------------------------------------------------

def load_data_from_gcs_simple(dataset_name, table_name, source, project=None):
    "load a *NEW* table to bq from gcs without the schema, autodetect it - nice!"
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
    "load a *NEW* table to bq from gcs with the schema"
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
        myErrors = load_job.errors
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
            "status": "complete",
            "msg": 'load_table_from_gcs {}:{} {}'.format(dataset_name, table_name, source)
        }

        return output_dict

    except Exception as e:
        errorStr = 'ERROR (load_table_from_gcs): ' + str(e)
        print(errorStr)
        raise

def load_table_from_csv(dataset_name, table_name, schema, local_file_name, skip_leading_rows=1, source_format='CSV', project=None):
    "load a local file to bq"
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


# ------------------------------------------------------------------
# export functions
# ------------------------------------------------------------------

def export_table_to_gcs(dataset_name, table_name, destination, field_delimiter=",", print_header=None, destination_format="CSV", compression="GZIP", project=None):
    try:
        """export a table from bq into a file on gcs,
            the destination should look like the following, with no brackets {}
            gs://{bucket-name-here}/{file-name-here}
            python export_data_to_gcs.py example_dataset example_table gs://example-bucket/example-data.csv
            The dataset and table should already exist
            py> myMsg = bqTools.export_data_to_gcs('rmurnane_dev01', 'dummy_data13',
                            'gs://merkle-cloud-innov-01-gcp/fake-dummy_data13_201711211526.csv',
                            field_delimiter="|", print_header=True, destination_format="CSV",
                            compression="GZIP")
        """
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


# ------------------------------------------------------------------
# view functions
# ------------------------------------------------------------------

def create_view(dataset_name, view_name, sqlQuery, project=None):
    "create a view via python"
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
# gcs functions - non bq but added just for reference
# ------------------------------------------------------------------

# Michael L. asked to research reading a file from gcs in python
def read_gcs_file(bucket_name='merkle-cloud-innov-01-gcp', blob_name='fake-data-3cols_2017110901.csv'):
    "reading a file from gcs in python"
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = storage.Blob(blob_name, bucket)
    content = blob.download_as_string()

    # content is one very long string
    # use split on newline characters to read by line
    # lines = content.split('\n')[:2]   #this is just top two lines
    lines = content.split('\n')
    for row in lines:
        print (row)

    n = len(lines)

    output_dict = {
        "bucket_name": bucket_name,
        "blob_name": blob_name,
        "status": "complete",
        "msg": 'read_gcs_file {} has {} records'.format(blob_name, n)
    }

    return output_dict


# ------------------------------------------------------------------
# TODO
# ------------------------------------------------------------------

# TODO:  fixed width file loads
