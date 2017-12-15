# gsTools.py
"""
Name:  
    gsTools.py 

Objectives:
    Help you get $h!t done in Google Cloud Storage using Python

TODO:
    decide if things should be called blobs or files, discuss with Mike

Problems?:
    Contact Rich or Tam

"""
import google.auth
from google.cloud import storage
import pandas as pd


# ------------------------------------------------------------------
# google cloud storage (gcs) aka google storage (gs) functions 
# ------------------------------------------------------------------


def copy_blob(bucket_name, blob_name, new_bucket_name, new_blob_name, project=None):
    """
    Copies a blob from one bucket to another with a new name.
    """
    try:
        # the read_gbq requires the project_id(project name), so fetch it if none passed in
        if not project:
            credentials, project_id = google.auth.default(scopes=['https://www.googleapis.com/auth/iam'])
            project = str(project_id)

        client = storage.Client(project=project)
        source_bucket = client.get_bucket(bucket_name)
        source_blob = source_bucket.blob(blob_name)
        # note:  (x or y) will is the IfNull/IsNull equiv in python, 
        # if x has a value, it will be used, otherwise y
        destination_bucket = client.get_bucket(new_bucket_name or bucket_name)

        new_blob = source_bucket.copy_blob(
            source_blob, destination_bucket, new_blob_name)

        msg = 'Blob {} in bucket {} copied to blob {} in bucket {}.'.format(
            source_blob.name, source_bucket.name, new_blob.name, destination_bucket.name)

        output_dict = {
            "project": str(project),
            "bucket_name": str(bucket_name),
            "blob_name": str(blob_name),
            "new_bucket_name": str(new_bucket_name),
            "new_blob_name": str(new_blob_name),
            "status": "complete",
            "msg": msg
        }

        return output_dict

    except Exception as e:
        errorStr = 'ERROR (copy_blob): ' + str(e)
        print(errorStr)
        raise


def rename_blob(bucket_name, blob_name, new_name, project=None):
    """
    Renames a blob.
    """
    try:
        # the read_gbq requires the project_id(project name), so fetch it if none passed in
        if not project:
            credentials, project_id = google.auth.default(scopes=['https://www.googleapis.com/auth/iam'])
        else:
            project_id = None

        project = (project or project_id)
        client = storage.Client(project=project)
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)

        new_blob = bucket.rename_blob(blob, new_name)

        if new_blob:
            new_blob_str = "YES"
        else:
            new_blob_str = "NO"

        msg = 'Blob {} has been renamed to {}'.format(blob_name, new_name)

        output_dict = {
            "project": str(project),
            "bucket_name": str(bucket_name),
            "blob_name": str(blob_name),
            "new_name": str(new_name),
            "status": "complete",
            "msg": msg,
            "new_blob_str": new_blob_str
        }

        return output_dict

    except Exception as e:
        errorStr = 'ERROR (rename_blob): ' + str(e)
        print(errorStr)
        raise


def new_gs_file_from_string(bucket_name, blob_name, string_text, project=None):
    """
    create a new file on gs and place the string in it
    """
    try:
        # the read_gbq requires the project_id(project name), so fetch it if none passed in
        if not project:
            credentials, project_id = google.auth.default(scopes=['https://www.googleapis.com/auth/iam'])
        else:
            project_id = None

        project = (project or project_id)
        client = storage.Client(project=project)
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_string(string_text, content_type='text/plain', client=client)
        msg = 'Upload to {} {} complete'.format(bucket_name, blob_name)

        output_dict = {
            "project": str(project),
            "bucket_name": str(bucket_name),
            "blob_name": str(blob_name),
            "new_name": str(string_text),
            "status": "complete",
            "msg": msg
        }

        return output_dict

    except Exception as e:
        errorStr = 'ERROR (new_gs_file_from_string): ' + str(e)
        print(errorStr)
        raise


def print_bucket_list(project=None):
    """
    simply print out the names of the buckets
    """
    try:
        # the read_gbq requires the project_id(project name), so fetch it if none passed in
        if not project:
            credentials, project_id = google.auth.default(scopes=['https://www.googleapis.com/auth/iam'])
        else:
            project_id = None

        project = (project or project_id)
        client = storage.Client(project=project)
        buckets = client.list_buckets()
        for bkt in buckets:
            print(bkt)

        msg = 'print complete'

        output_dict = {
            "status": "complete",
            "msg": msg
        }

        return output_dict

    except Exception as e:
        errorStr = 'ERROR (print_bucket_list): ' + str(e)
        print(errorStr)
        raise


def get_blob_list_dataframe(bucket_name, max_results=99999, prefix=None, project=None, printOut=None):
    """
    return a pandas dataframe of the filenames in a bucket, search for files by using prefix
    """
    try:
        if not project:
            credentials, project_id = google.auth.default(scopes=['https://www.googleapis.com/auth/iam'])
        else:
            project_id = None

        project = (project or project_id)
        client = storage.Client(project=project)
        bucket = client.get_bucket(bucket_name)
        blobs = bucket.list_blobs(max_results=max_results, prefix=prefix)
        output_dict = []
        for blobFile in blobs:
            # other attributes of the blob object: https://cloud.google.com/storage/docs/json_api/v1/objects
            fileName = blobFile.name
            fileId = blobFile.id
            fileSize = blobFile.size
            time_created = blobFile.time_created
            updated = blobFile.updated
            if printOut:
                print(fileName)
            output_dict.append({'fileName': fileName, 'fileId': fileId, 'fileSize': fileSize,
                                'time_created': time_created, 'updated': updated})

        df = pd.DataFrame(output_dict)

        return df

    except Exception as e:
        errorStr = 'ERROR (get_blob_list): ' + str(e)
        print(errorStr)
        raise


def read_gcs_file(bucket_name='blah-blah-blah', blob_name='fake-data-3cols_2017110901.csv'):
    """
    reading a file from gcs in python
    Michael L. asked to research reading a file from gcs in python
    """
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


def upload_file(local_filename, bucket_name, gs_filename):
    """
    upload a file to gs
    https://cloud.google.com/storage/docs/object-basics#storage-upload-object-python
    """
    try:
        client = storage.Client()
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(gs_filename)
        blob.upload_from_filename(local_filename)

        msg = 'local_filename {} has been sent to {} {} '.format(local_filename, bucket_name, gs_filename)

        output_dict = {
            "bucket_name": str(bucket_name),
            "local_filename": str(local_filename),
            "gs_filename": str(gs_filename),
            "status": "complete",
            "msg": msg
        }

        return output_dict

    except Exception as e:
        errorStr = 'ERROR (upload_file): ' + str(e)
        print(errorStr)
        raise


def download_file(bucket_name, gs_filename, local_filename):
    """
    upload a file to gs
    https://cloud.google.com/storage/docs/object-basics#storage-upload-object-python
    """
    try:
        client = storage.Client()
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(gs_filename)
        blob.download_to_filename(local_filename)

        msg = 'gcp file {} {} has been sent to {} '.format(bucket_name, gs_filename, local_filename)

        output_dict = {
            "bucket_name": str(bucket_name),
            "gs_filename": str(gs_filename),
            "local_filename": str(local_filename),
            "status": "complete",
            "msg": msg
        }

        return output_dict

    except Exception as e:
        errorStr = 'ERROR (download_file): ' + str(e)
        print(errorStr)
        raise
