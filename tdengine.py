import time
import json
from datetime import datetime, timezone
from unittest import case
import uuid
import copy
import taosrest
import logging
from fledge.common import logger
from fledge.services.south import exceptions


#To-Do
  #get units in here somehow
  #Consider changing the inserts to a OpenTSDB protocol, maybe some queries too. Hopefully will reduce io overhead
  #Make sure SQL string inputs are safe, don't contain escape characters, tdengine disallowed characters/terms, etc.
  #Add more data types?
  #Accommodate sub-millisecond timestamp precision

#Max SQL query length is 65480, according to https://tdengine.com/docs/en/v2.0/taos-sql
#Interesting aside, if you do INSERT INTO tablename USING sTableName, and tablename already exists under a different supertable. TDEngine won't complain and just write to the existing table.

#TDEngine Feedback: Would be helpful if a whole compounded insert statement wouldn't bail at the first issue.

_LOGGER = logger.setup(__name__,level=logging.INFO)
# _LOGGER = logger.setup(__name__)  <-- I asked abou this in Fledge slack, but still not sure why this works in other plugins but not this one

global assetList #I am not 100% sure that multiple plugin instances won't fight over a variable declared here

_DEFAULT_CONFIG = {
    'plugin': {
        'description': 'Python module name of the plugin to load',
        'type': 'string',
        'default': 'tdengine'
    },
    "source": {
         "description": "Source of data to be sent on the stream. May be either readings or statistics.",
         "type": "enumeration",
         "default": "readings",
         "options": [ "readings", "statistics" ],
         'displayName': 'Source'
    },
    'url': {
        'description': 'URL of the TDengine instance',
        'type': 'string',
        'default': 'https://gw.us-east-1.aws.cloud.tdengine.com',
        'displayName': 'URL'
    },
    'token': {
        'description': 'Token for the TDengine instance',
        'type': 'string',
        'default': '',
        'displayName': 'Token'
    },
    'database': {
        'description': 'TDEngine database to write to',
        'type': 'string',
        'default': 'Fledge',
        'displayName': 'Database Name'
    },
    'pluginId': {
        'description': 'This will populate the pluginID tag of created TDengine tables',
        'type': 'string',
        'default': 'Plant 1',
        'displayName': 'Plugin ID'
    }

}


def plugin_info():
    return {
        'name': 'tdengine',
        'version': '1.0',
        'type': 'north',
        'interface': '1.0',
        'config': _DEFAULT_CONFIG
    }


def plugin_init(data):
    global conn
    global assetList
    assetList =[]

    config = data
    _LOGGER.info('TDEngine plugin with ID '+ config['pluginId']['value'] + ' starting up!')
    url = config['url']['value']
    token = config['token']['value']
    database = config['database']['value']

    conn = taosrest.connect(url=url, token=token)
    #conn.execute('CREATE DATABASE IF NOT EXISTS '+database)     Apparently tdengine cloud does not yet allow remote database creation

    #Max SQL statement size is configurable in tdengine I think, so maybe query and get that here
    conn.execute('CREATE STABLE IF NOT EXISTS '+database+'.fledge_float (ts TIMESTAMP, val FLOAT, quality INT) TAGS (reading BINARY(64), pluginId BINARY(64))')
    conn.execute('CREATE STABLE IF NOT EXISTS '+database+'.fledge_int (ts TIMESTAMP, val INT, quality INT) TAGS (reading BINARY(64), pluginId BINARY(64))') 
    conn.execute('CREATE STABLE IF NOT EXISTS '+database+'.fledge_bool (ts TIMESTAMP, val BOOL, quality INT) TAGS (reading BINARY(64), pluginId BINARY(64))') 
    conn.execute('CREATE STABLE IF NOT EXISTS '+database+'.fledge_str (ts TIMESTAMP, val BINARY(64), quality INT) TAGS (reading BINARY(64), pluginId BINARY(64))') 


    return config
    


async def plugin_send(handle, payload, stream_id):
    _LOGGER.info('Sending readings to TDengine')
    global conn
    global assetList

    is_data_sent=False
    last_object_id = 0
    num_sent=0
    createTableString=''
    insertList=[]
    insertListLength=0

    #To-do: If disconnected, reconnect
    # try: 
    #   conn.execute('SHOW DATABASES')
    # except:
    #   conn = taosrest.connect(url=handle['url']['value'], token=handle['token']['value'])

    dataTypeTranslator={"float":"float", "int":"int", "bool":"bool", "str":"binary(64)"}
    database=handle['database']['value']
    pluginId=handle['pluginId']['value']

    payload_sorted=sorted(payload, key=lambda i: (i['asset_code'],i['user_ts'])) #Supposed to make sure readings are sorted by asset code, and timestamp ascending

    # _LOGGER.info('Create Table Check')
    lastassetname=''
    for p in payload_sorted:
        asset_code=p['asset_code']
        if asset_code!=lastassetname:
            readings=p['reading']     
            if asset_code not in assetList:
                for readingName,value in readings.items(): 
                    valueType =  type(value).__name__
                    createTableString=createTableString + database+"."+asset_code+"_"+readingName+" USING "+database+".fledge_"+valueType+" TAGS ('"+readingName+"','"+pluginId+"') "
                assetList.append(asset_code)
                if len(createTableString)>60000:
                    createTables(createTableString)
                    createTableString=''
        
        lastassetname=asset_code
    
    if len(createTableString)>0:
        _LOGGER.info('Create Table Submit')
        createTables(createTableString)

    for p in payload_sorted:
        try:
            asset_code=p['asset_code']
            readings=p['reading']     
            timestamp = str(datetime.strptime(p['user_ts'],'%Y-%m-%dt%H:%M:%S.%fz'))[:-3] #Format timestamp and chop off sub-millisecond
            insertString=''
            for readingName, value in readings.items():
                valueString=str(value)
                insertString = insertString + database+"." + asset_code+"_"+readingName+" VALUES ('"+ timestamp + "', " + valueString + ", 0) "
            if insertListLength+len(insertString)<65465:     #might be able to use insertList.__len__ instead of dedicated variable
                insertList.append(insertString)
                insertListLength=insertListLength+len(insertString)
            else:
                #_LOGGER.info("Max insert statement length reached at "+str(p))
                success=insertReadings(insertList)
                is_data_sent=success or is_data_sent
                insertList=[insertString]
                insertListLength=len(insertString)

            num_sent+=1
            last_object_id = p['id']
            
        except Exception as e:
            _LOGGER.info("Paylod Loop Exception! Reading "+str(p)+" --> "+str(e))

    _LOGGER.info('Sending Insert String')
    if insertList.count>0:
        success=insertReadings(insertList,payload)  #It's possible the whole query fails, or one of the readings errors out and causes the rest not to be written. Not accounted for right now
        is_data_sent=success or is_data_sent 

    #_LOGGER.info('Events sent: '+str(num_sent)+" of "+str(len(payload))+" Data sent: "+str(is_data_sent)+" id: "+str(last_object_id))
    return is_data_sent, last_object_id, num_sent
    


def plugin_reconfigure(handle, new_config):
    #idk why but the other north plugins don't do this, say it's not possible to reconfig north

    #new_handle = copy.deepcopy(new_config)  
    #return new_handle
    pass
    
    


def plugin_shutdown(handle):
    global conn
    conn.close()

################################################################################################################
    
def createTables(createTableString):
    try:
        conn.execute("CREATE TABLE IF NOT EXISTS "+createTableString)
    except Exception as e:
        if "0x0603" in str(e):  #0x0603 means Table already exists
            _LOGGER.info("Plugin attempted to create tables that already exist") #It's possible some uncreated tables later in the string got ignored. Might need to handle that here
        else:
            raise(e)
            

def insertReadings(insertList):
    submitString=''
    success=False
    for i in insertList:
        submitString=submitString+i
    try:
        conn.execute("INSERT INTO "+submitString)
        success=True
    except Exception as e:
        _LOGGER.info("Insert Exception ---> "+str(e))
        #experimental attempt at inserting recoverable data. Not needed once we move away from SQL inserts to write data.
        #Maybe check here if exception is a type that warrents a split/retry (skip if connection exception, etc.)
        #if insertList.__len__()>1:
        #    mid=int(insertList.__len__()/2)
        #    _LOGGER.info("Submitting split list of mid "+str(mid))
        #    list1=insertList[:mid] #I thought this would create ovelap but doesn't
        #    list2=insertList[mid:]
        #    success1=insertReadings("INSERT INTO "+list1)
        #    success2=insertReadings("INSERT INTO "+list2)
        #    success=success1 or success2
    return success
