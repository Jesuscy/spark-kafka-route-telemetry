from asyncio.log import logger
import datetime
import os
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient
from azure.storage.filedatalake import DataLakeFileClient
from azure.core.exceptions import AzureError
from dotenv import load_dotenv

#Esta es una utilidad para subir archivos al datalake de Azure, ahora está en desuso.

load_dotenv()


def get_datalake_connection(layer:str):
    try:
        credential = ClientSecretCredential(
        tenant_id=os.getenv("TENANT_ID"),
        client_id=os.getenv("CLIENT_ID"),
        client_secret=os.getenv("CLIENT_SECRET"))
    
    except AzureError as e:
        logger.error(f"Credential error:  {e}")
        
    try:
        service_client = DataLakeServiceClient(account_url=os.getenv("STORAGE_ACCOUNT_URL"), credential=credential)
        logger.info(f"******* Connected to Data Lake successfully *********")
        
    except AzureError as e:
        logger.error(f"Error connecting to Data Lake {e}")
        raise e		
    
    try: 
        file_system_client = service_client.create_file_system(f"trips-info-container-{layer}")
        logger.info(f"******* Filesystem created *********")  

    except AzureError as e:
        file_system_client =  service_client.get_file_system_client(f"trips-info-container-{layer}")		
        logger.info(f"******* Filesystem existed *********")

        return file_system_client


def upload_to_datalake(name:str, data, layer:str):

    file_system_client = get_datalake_connection(layer)
    
    try:
        data_dir = file_system_client.create_directory("data")
        logger.info(f"******* Directory created *********")

    except AzureError as e:
        data_dir = file_system_client.get_directory_client("data")
        logger.info(f"******* Directory existed *********")
    
    
    try:
        data_date_part_dir = data_dir.create_sub_directory(datetime.now().strftime('%Y-%m-%d'))
        logger.info(f"******* Directory created *********")

    except AzureError as e:
        data_date_part_dir = data_dir.get_sub_directory_client(datetime.now().strftime('%Y-%m-%d'))
        logger.info(f"******* Directory existed *********")
        
    try:
        file_client = data_date_part_dir.create_file(f"{name}_{datetime.now().strftime('%Y%m%d%H%M%S')}.json")
        file_client.append_data(data, offset=0, length=len(data))
        file_client.flush_data(len(data))
        logger.info("Data uploaded to Data Lake successfully.")
            
    except AzureError as e:
        logger.error(f"Error uploading file to Data Lake {e}")
        raise e