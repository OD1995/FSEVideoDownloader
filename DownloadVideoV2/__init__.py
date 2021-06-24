# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import logging
import youtube_dl
import os
from azure.storage.blob import BlobClient, ContentSettings

def main(dst1Outputs: dict) -> str:
    with youtube_dl.YoutubeDL() as ydl:
        res1 = ydl.extract_info(
            dst1Outputs['url'],
            download=False
        )
        logging.info(f"res1: {res1}")
    
    ## Set the path of the video
    blobName = dst1Outputs['vidName'] + ".mp4"
    output_path = "/tmp/" + blobName
    ydl_opts = {
        'outtmpl': output_path,
        'retries' : 100
    }
    
    ## Download the video
    with youtube_dl.YoutubeDL(ydl_opts) as ydl:
        res2 = ydl.download([dst1Outputs['url']])
        logging.info(f"res2: {res2}")

    logging.info("download finished")

    blobClient = BlobClient.from_connection_string(
        conn_str=os.getenv("fsevideoCS"),
        container_name="azure-video-to-image-import",
        blob_name=blobName
    )
    logging.info("blobClient created")
    with open(output_path,"rb") as data:
        blobClient.upload_blob(
            data
        )
    logging.info("blob created")

    return 'done'
