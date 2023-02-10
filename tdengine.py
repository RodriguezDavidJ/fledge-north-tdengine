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
    _LOGGER.info('Plugin send begin')
    global conn
    global assetList

    is_data_sent=False
    last_object_id = 0
    num_sent=0
    createTableString=''
    insertString=''

    #To-do: If disconnected, reconnect
    # try: 
    #   conn.execute('SHOW DATABASES')
    # except:
    #   conn = taosrest.connect(url=handle['url']['value'], token=handle['token']['value'])

    #dataTypeTranslator={"float":"float", "int":"int", "bool":"bool", "str":"binary(64)"}
    database=handle['database']['value']
    pluginId=handle['pluginId']['value']

    payload_sorted=sorted(payload, key=lambda i: (i['asset_code'],i['user_ts'])) #Supposed to make sure readings are sorted by asset code, and timestamp ascending

    #Build insert string from payload
    _LOGGER.info('Parsing payload')
    for p in payload_sorted:
        try:
            asset_code=p['asset_code']
            readings=p['reading']     
            timestamp = str(datetime.strptime(p['user_ts'],'%Y-%m-%dt%H:%M:%S.%fz'))[:-3] #Format timestamp and chop off sub-millisecond
            for readingName, value in readings.items():
                valueType =  type(value).__name__
                valueString=str(value)
                insertString = insertString + database+"." + asset_code+"_"+readingName+" USING "+database+".fledge_"+valueType+" TAGS ('"+readingName+"','"+pluginId+"')" + " VALUES ('"+ timestamp + "', " + valueString + ", 0) "
            if len(insertString)>62000: #Could theoretically overshoot the 65k limit if the next string to be added is very long
                #_LOGGER.info("Max insert statement length reached at "+str(p))
                success=insertReadings(insertString)
                is_data_sent=success or is_data_sent
                insertString=''

            num_sent+=1
            last_object_id = p['id']
            
        except Exception as e:
            _LOGGER.info("Paylod Loop Exception! Reading "+str(p)+" --> "+str(e))

    _LOGGER.info('Checking for data to insert')
    if len(insertString)>0:
        _LOGGER.info('Inserting data')
        success=insertReadings(insertString)  #It's possible the whole query fails, or one of the readings errors out and causes the rest not to be written. Not accounted for right now
        is_data_sent=success or is_data_sent 

    _LOGGER.info('Done! Events sent: '+str(num_sent)+" of "+str(len(payload))+" Data sent: "+str(is_data_sent)+" id: "+str(last_object_id))
    return is_data_sent, last_object_id, num_sent
    


def plugin_reconfigure(handle, new_config):
    #idk why but the other north plugins don't do this, say it's not possible to reconfig north

    #new_handle = copy.deepcopy(new_config)  
    #return new_handle
    pass
    
    


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
