from secrets import source_conn_str
from secrets import destination_conn_str
import os, uuid
import sys 
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions 
from azure.identity import DefaultAzureCredential
import pandas as pd 
from secrets import source_conn_str 
from secrets import destination_conn_str 

try:
    # we connect ourselves to the source in azure: 
    blob_service_client = BlobServiceClient.from_connection_string(source_conn_str) 
    containers_list = blob_service_client.list_containers() 
    list_of_containers = [] 
    for container in containers_list:
        list_of_containers.append(container.name)

    # we download anything from a source container of there 
    container_name = list_of_containers[3] 
    blob = str(container_name) + '/staging/VILLO_PARQUET/year=2021/month=10/day=1/villo_2021-10-01T00:22:37.1633047757.parquet'
    local_file_name = 'stagingdestination' # this is the file location in this computer
    local_path= "./data" # this is the folder location in this computer
    download_file_path = os.path.join(local_path, local_file_name) # this is where the file is put on the computer, it's only a temporary measure until the download phase is mastered and I can directly send it to the upload phase
    container_client = blob_service_client.get_container_client(container= container_name) 
    with open(file=download_file_path, mode="wb") as download_file: 
        download_file.write(container_client.download_blob(blob)).readall()

except Exception as ex:
    print('Exception:')
    print(ex)
