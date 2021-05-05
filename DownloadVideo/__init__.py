# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import logging
from azure.storage.blob import BlockBlobService
from azure.storage.blob.models import ContentSettings
import streamlink
import shutil
from streamlink.stream.ffmpegmux import MuxedStream


def main(inputs: dict) -> str:
    logging.info("DownloadVideo started")
    ## Vars to use
    varsToUse = {}
    ## Get video URL
    varsToUse['vidURL'] = inputs['url']
    ## Decide on video name
    if inputs['name'] is None:
        varsToUse["vidName"] = ""
    else:
        varsToUse["vidName"] = inputs['name']
    ## Output container
    varsToUse


    ## Get all streams of the video
    allStreams = streamlink.streams(vidURL)
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

    outBBS = BlockBlobService(
            connection_string=outCS
        )
    logging.info("about to create blob")
    fd = stream.open()
    outBBS.create_blob_from_stream(
            container_name="video-from-stream", 
            blob_name=f"{vidName}.mp4", 
            stream=fd,
            max_connections=1,
            use_byte_buffer=True,
            content_settings=ContentSettings(
                content_type="video/mp4"
            )
        )