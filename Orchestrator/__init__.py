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
    ## DoSQLThings1  - change 'Status' column to "In Progress" and 'LastStatusUpdate'
    ##               - add row to AzureBlobVideos after deciding video name
    result1 = yield context.call_activity('DoSQLThings1', "Tokyo")
    ## DownloadVideo - download video to 'video-from-stream' container
    ##               - copy blob to 'azure-video-to-image-import' container
    result2 = yield context.call_activity('DownloadVideo', "Seattle")
    ## DoSQLThings2  - change 'Status' column to "Completed" and 'LastStatusUpdate'
    result3 = yield context.call_activity('DoSQLThings2', "Seattle")

    return [result1, result2, result3]

main = df.Orchestrator.create(orchestrator_function)