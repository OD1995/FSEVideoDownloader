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
    get_video_name
)


def main(dstInputs: dict) -> str:
    ## Update Status column
    update_VideosToDownload(
        rowID=dstInputs['rowID'],
        newStatus="In Progress"
    )
    ## Create dict to return
    dtr = {}
    ## Decide video name to use
    if dstInputs['name'] is None:
        dtr['vidName'] = get_video_name(dstInputs['url'])
    else:
        dtr['vidName'] = dstInputs['name']