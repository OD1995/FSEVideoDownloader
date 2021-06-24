import requests
import sys
sys.path.insert(0,r"K:\Technology Team\Python")
import sql_helper

def bool_to_int(b):
    if b == True:
        return 1
    elif b == False:
        return 0
    else:
        raise ValueError(f'{b} is neither True nor False')

def add_row_and_trigger(
    VideoURL : str,
    Event : str,
    Sport : str,
    EndpointId : str,
    MultipleVideoEvent : bool,
    SamplingProportion : float,
    AudioTranscript : bool,
    DatabaseID : str
        ):
    if (
        (VideoURL is None) or
        (Sport is None)
    ):
        raise ValueError('one of VideoURL or Sport is None, not allowed')
    
    VideoURL = f"'{VideoURL}'"
    Event = 'NULL' if Event is None else f"'{Event}'"
    Sport = f"'{Sport}'"
    EndpointId = 'NULL' if EndpointId is None else f"'{EndpointId}'"
    MultipleVideoEvent = 'NULL' if MultipleVideoEvent is None else bool_to_int(MultipleVideoEvent)
    SamplingProportion = 'NULL' if SamplingProportion is None else SamplingProportion
    AudioTranscript = 'NULL' if AudioTranscript is None else bool_to_int(AudioTranscript)
    DatabaseID = 'NULL' if DatabaseID is None else f"'{DatabaseID}'"
    
    
    Q = f"""
INSERT INTO VideosToDownload (
    VideoURL,
    Event,
    Sport,
    EndpointId,
    MultipleVideoEvent,
    SamplingProportion,
    AudioTranscript,
    DatabaseID
)
VALUES  (
    {VideoURL},
    {Event},
    {Sport},
    {EndpointId},
    {MultipleVideoEvent},
    {SamplingProportion},
    {AudioTranscript},
    {DatabaseID}      
)
    """
    print(Q)
    
    sql_helper.sqlCommand(
        query=Q,
        servername="inf",
        database="AzureCognitive"
    )
    
    Q2 = f"""
    SELECT * FROM VideosToDownload
    WHERE 
        VideoURL = {VideoURL} AND
        Event = {Event} AND
        Sport = {Sport}
    ORDER BY DateTimeRowAdded DESC
    """
    df = sql_helper.fromSQLquery(
        query=Q2,
        servername="inf",
        database="AzureCognitive"
    )
    params = {
        'rowID' : df.loc[0,'RowID'],
        'url' : df.loc[0,'VideoURL'],
        'name' : df.loc[0,'Event'],
        'sport' : df.loc[0,'Sport'],
        'endpointID' : df.loc[0,'EndpointId'],
        'multipleVideoEvent' : df.loc[0,'MultipleVideoEvent'],
        'samplingProportion' : df.loc[0,'SamplingProportion'],
        'audioTranscript' : df.loc[0,'AudioTranscript'],
        'databaseID' : df.loc[0,'DatabaseID']          
            }
    
    r = requests.get(
        url='https://fsevideodownloader.azurewebsites.net/api/HttpTrigger',
        params=params
    )
    
    print(r.json()['statusQueryGetUri'])
    
    
add_row_and_trigger(
    VideoURL="https://www.bbc.co.uk/iplayer/episode/p09lg74t/wimbledon-qualifying-2021-day-two-including-gbs-mchugh",
    Event="WimbledonQualifyingTest2",
    Sport="test",
    EndpointId=None,
    MultipleVideoEvent=False,
    SamplingProportion=None,
    AudioTranscript=False,
    DatabaseID=None
)
    
    
    
    
    