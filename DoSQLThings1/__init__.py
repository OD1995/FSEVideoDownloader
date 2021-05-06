# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import logging
from MyFunctions import (
    update_VideosToDownload,
    get_video_name,
    add_row_to_AzureBlobVideos
)


def main(dst1Inputs: dict) -> dict:
    ## Update Status column
    update_VideosToDownload(
        rowID=dst1Inputs['rowID'],
        newStatus="In Progress"
    )
    ## Create dict to return
    dtr = {
        "url" : dst1Inputs["url"]
    }
    ## Decide video name to use
    if dst1Inputs['name'] is None:
        dtr['vidName'] = get_video_name(dst1Inputs['url'])
    else:
        dtr['vidName'] = dst1Inputs['name']
    ## Add row to AzureBlobVideos SQL table
    eID = "NULL" if dst1Inputs['endpointID'] is None else dst1Inputs['endpointID']
    mve = "NULL" if dst1Inputs['multipleVideoEvent'] is None else dst1Inputs['multipleVideoEvent']
    sp = "NULL" if dst1Inputs['samplingProportion'] is None else dst1Inputs['samplingProportion']
    at = "NULL" if dst1Inputs['audioTranscript'] is None else dst1Inputs['audioTranscript']
    add_row_to_AzureBlobVideos(
        vidName=dtr['vidName'],
        sport=dst1Inputs['sport'],
        endpointID=eID,
        multipleVideoEvent=mve,
        samplingProportion=sp,
        audioTranscript=at
    )
    return dtr