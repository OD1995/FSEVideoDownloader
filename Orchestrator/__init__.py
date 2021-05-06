# This function is not intended to be invoked directly. Instead it will be
# triggered by an HTTP starter function.
# Before running this sample, please:
# - create a Durable activity function (default name is "Hello")
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import logging
import json

import azure.functions as func
import azure.durable_functions as df


def orchestrator_function(context: df.DurableOrchestrationContext):
    ## Get inputs from trigger
    dst1Inputs = json.loads(context._input)
    logging.info(f"dst1Inputs: {dst1Inputs}")
    ## DoSQLThings1  - change 'Status' column to "In Progress" and 'LastStatusUpdate'
    ##               - add row to AzureBlobVideos after deciding video name
    dst1Outputs = yield context.call_activity('DoSQLThings1', dst1Inputs)
    logging.info(f"dst1Outputs: {dst1Outputs}")
    ## DownloadVideo - download video to 'video-from-stream' container
    ##               - copy blob to 'azure-video-to-image-import' container
    dvOutput = yield context.call_activity('DownloadVideo', dst1Outputs)
    logging.info(f"dvOutput: {dvOutput}")
    ## DoSQLThings2  - change 'Status' column to "Completed" and 'LastStatusUpdate'
    dst2Output = yield context.call_activity('DoSQLThings2', dst1Inputs)
    logging.info(f"dst2Output: {dst2Output}")

    return "completed"

main = df.Orchestrator.create(orchestrator_function)