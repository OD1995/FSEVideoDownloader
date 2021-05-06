# This function an HTTP starter function for Durable Functions.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable activity function (default name is "Hello")
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt
 
import logging

import azure.functions as func
import azure.durable_functions as df


async def main(req: func.HttpRequest, starter: str) -> func.HttpResponse:
    client = df.DurableOrchestrationClient(starter)
    ## Get inputs
    inputs = {}
    # for a,b in []:
        # inputs[a] = req.params.get(b)
    for f in [
        "rowID",
        "url",
        "name",
        "sport",
        "endpointID",
        "multipleVideoEvent",
        "samplingProportion",
        "audioTranscript"
    ]:
        inputs[f] = req.params.get(f)
    ## Start orchestrator
    instance_id = await client.start_new(
        orchestration_function_name="Orchestrator",
        instance_id=None,
        client_input=inputs
    )

    logging.info(f"Started orchestration with ID = '{instance_id}'.")

    return client.create_check_status_response(req, instance_id)