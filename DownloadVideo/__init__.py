# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import logging
# from azure.storage.blob import BlockBlobService
# from azure.storage.blob.models import ContentSettings
from azure.storage.blob import BlobClient, ContentSettings
import streamlink
import shutil
from streamlink.stream.ffmpegmux import MuxedStream
import os

def main(dst1Outputs: dict) -> str:
    logging.info("DownloadVideo started")
    ## Get all streams of the video
    allStreams = streamlink.streams(dst1Outputs["url"])
    logging.info(f"allStreams: {len(allStreams)}")
    nonEmptyStreams = {
                k:v
                for k,v in allStreams.items()
                if not isinstance(v,MuxedStream)
            }
    logging.info(f"allStreams: {len(nonEmptyStreams)}")
    quals = [
            'best',
            '1080p',
            '720p', 
            '480p', 
            '360p', 
            '240p', 
            '144p',
        ]
    ## Raise error if there are no suitable video qualities
    if all([x not in nonEmptyStreams for x in quals]):
        raise ValueError("No suitable video qualities available")

    ## Otherwise, identify the best one
    while True:
        for q in quals:
            if q in nonEmptyStreams:
                qualToUse = q
                break
        break
    logging.info(f"qualToUse: {qualToUse}")
    stream = nonEmptyStreams[qualToUse]

    # outBBS = BlockBlobService(
    #         connection_string=os.getenv("fsevideoCS")
    #     )
    blobName = f"{dst1Outputs['vidName']}.mp4"
    blobClient = BlobClient.from_connection_string(
        conn_str=os.getenv("fsevideoCS"),
        container_name="video-from-stream",
        blob_name=blobName
    )
    ## Create blob in "video-from-stream" container initially
    logging.info("about to create blob")
    fd = stream.open()
    blobClient.upload_blob(
        data=fd,
        overwrite=True,
        content_settings=ContentSettings(
            content_type="video/mp4"
        )
    )
    # outBBS.create_blob_from_stream(
    #         container_name="video-from-stream", 
    #         blob_name=blobName, 
    #         stream=fd,
    #         max_connections=1,
    #         # use_byte_buffer=True,
    #         content_settings=ContentSettings(
    #             content_type="video/mp4"
    #         )
    #     )
    ## Then copy blob over to "azure-video-to-image-import" container
    ##    - this is to stop the event grid trigger firing before the whole
    ##     blob has been created initially
    ##    - this early triggering is just a theory, hasn't been tested but
    ##     due to requirements to get the tool built, it was assumed this would
    ##     cause issues
    # copySource = f"https://fsevideos.blob.core.windows.net/video-from-stream/{blobName}"
    # outBBS.copy_blob(
    #     container_name="azure-video-to-image-import",
    #     blob_name=blobName,
    #     copy_source=copySource
    # )

    return "done"