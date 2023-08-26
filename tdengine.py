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

_LOGGER = logger.setup(__name__,level=logging.INFO)
# _LOGGER = logger.setup(__name__)  <-- I asked abou this in Fledge slack, but still not sure why this works in other plugins but not this one

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
        'default': 'fledge',
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
    _LOGGER.info('TDEngine plugin info called')
    return {
        'name': 'tdengine',
        'version': '1.0',
        'type': 'north',
        'interface': '1.0',
        'config': _DEFAULT_CONFIG
    }
    


def plugin_init(data):
    global conn

    config = data
    _LOGGER.info('TDEngine plugin with ID '+ config['pluginId']['value'] + ' starting up!')
    url = config['url']['value']
    token = config['token']['value']
    database = config['database']['value']

    conn = taosrest.connect(url=url, token=token)
    
    #Max SQL statement size is configurable in tdengine I think, so maybe query and get that here
    conn.execute('CREATE STABLE IF NOT EXISTS '+database+'.fledge_float (ts TIMESTAMP, val FLOAT, quality INT) TAGS (reading BINARY(64), pluginId BINARY(64))')
    conn.execute('CREATE STABLE IF NOT EXISTS '+database+'.fledge_bool (ts TIMESTAMP, val BOOL, quality INT) TAGS (reading BINARY(64), pluginId BINARY(64))') 
    conn.execute('CREATE STABLE IF NOT EXISTS '+database+'.fledge_str (ts TIMESTAMP, val BINARY(64), quality INT) TAGS (reading BINARY(64), pluginId BINARY(64))') 


    return config
    


async def plugin_send(handle, payload, stream_id):
    _LOGGER.info('Plugin send begin')
    global conn

    is_data_sent=False
    last_object_id = 0
    last_object_id_attempt=0
    num_sent=0
    num_sent_attempt=0
    insertString=''


    database=handle['database']['value']
    pluginId=handle['pluginId']['value']

    #Build insert string from payload
    _LOGGER.info('Parsing payload')
    for p in payload:
        try:
            asset_code=p['asset_code']
            readings=p['reading']     
            timestamp = str(datetime.strptime(p['user_ts'],'%Y-%m-%d %H:%M:%S.%f%z').astimezone(timezone.utc))[:-3] #Format timestamp and chop off sub-millisecond
            for readingName, value in readings.items():
                valueType =  type(value).__name__
                if valueType=="int":        
                    valueType="float"
                if valueType=="str":
                    value="'"+value+"'"
                valueString=str(value)
                insertString = insertString + database+"." + asset_code+"_"+readingName+" USING "+database+".fledge_"+valueType+" TAGS ('"+readingName+"','"+pluginId+"')" + " VALUES ('"+ timestamp + "', " + valueString + ", 0) "
            
            num_sent_attempt+=1
            last_object_id_attempt = p['id']

            if len(insertString)>62000: #Could theoretically overshoot the 65k limit if the next string to be added is very long
                success=insertReadings(insertString)                
                if success:
                    num_sent=num_sent+num_sent_attempt
                    last_object_id=last_object_id_attempt
                num_sent_attempt=0
                is_data_sent=success or is_data_sent
                insertString=''


            
        except Exception as e:
            _LOGGER.info("Paylod Loop Exception! Reading "+str(p)+" --> "+str(e))

    if len(insertString)>0:
        success=insertReadings(insertString)  
        if success:
            num_sent=num_sent+num_sent_attempt
            last_object_id=last_object_id_attempt
        is_data_sent=success or is_data_sent

    _LOGGER.info('Done! Events sent: '+str(num_sent)+" of "+str(len(payload))+" Data sent: "+str(is_data_sent)+" id: "+str(last_object_id))
    return is_data_sent, last_object_id, num_sent
    
    
    


def plugin_shutdown(handle):
    _LOGGER.info('Fledge called plugin shutdown')
    global conn
    conn.close()

################################################################################################################   

def insertReadings(insertString):
    submitString=''
    success=False
    try:
        conn.execute("INSERT INTO "+insertString)
        success=True
    except Exception as e:
        _LOGGER.info("Insert Exception ---> "+str(e))
    return success
