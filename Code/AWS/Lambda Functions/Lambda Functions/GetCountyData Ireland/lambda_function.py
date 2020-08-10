import json
import logging
import pymysql

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': intent_name,
            'slots': slots,
            'slotToElicit': slot_to_elicit,
            'message': message
        }
    }
    
def close(session_attributes, fulfillment_state, message):
    response = {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }

    return response

def db_parameters():
    endpoint = 'aws-connect.mysql.database.azure.com'
    username = 'rocket@aws-connect'
    password = 'test@123'
    database_name = 'lextest'
    return endpoint, username, password, database_name
    

def get_db_cursor(query):
    endpoint, username, password, database_name = db_parameters()
    connection = pymysql.connect(endpoint, user=username,
    passwd=password, db=database_name)
    cursor = connection.cursor()
    cursor.execute(query)
    return cursor
    

def isvalid_county(county):
    all_counties = ['antrim','armagh','carlow','cavan',
    'clare','cork','derry','donegal','down','dublin','fermanagh',
    'galway','kerry','kildare','kilkenny','laois','leitrim','limerick',
    'longford','louth','mayo','meath','monaghan','offaly','roscommon','sligo',
    'tipperary','tyrone','waterford','westmeath','wexford','wicklow']
    return county.lower() in all_counties

def try_ex(func):
    """
    Call passed in function in try block. If KeyError is encountered return None.
    This function is intended to be used to safely access dictionary.

    Note that this function would have negative impact on performance.
    """

    try:
        return func()
    except KeyError:
        return None

def build_validation_result(isvalid, violated_slot, message_content):
    return {
        'isValid': isvalid,
        'violatedSlot': violated_slot,
        'message': {'contentType': 'PlainText', 'content': message_content}
    }

def get_county_data(intent_request):
    intent_name = intent_request['currentIntent']['name']
    county_name = intent_request['currentIntent']['slots']['county']
    logger.debug(county_name)
    logger.debug(isvalid_county(county_name))
    if county_name and not isvalid_county(county_name):
        validation_result = build_validation_result(
            False,
            'county',
            'The county {} is invalid, Can you try a different county'.format(county_name)
            )
        intent_request['currentIntent']['slots'][validation_result['violatedSlot']] = None
        return elicit_slot(
            {},
            intent_request['currentIntent']['name'],
            intent_request['currentIntent']['slots'],
            validation_result['violatedSlot'],
            validation_result['message']
        )
    else:
        county = intent_request['currentIntent']['slots']['county']
        #typeofCase = intent_request['currentIntent']['slots']['TypeOfCases']
        date = intent_request['currentIntent']['slots']['Date']
        query = "Select " +county+ " from countywisetimeseries where dates='"+date+"'"
        figure = get_db_cursor(query).fetchall()[0]
        
        return close(
            {},
            'Fulfilled',
            {
                'contentType': 'PlainText',
                'content': 'There were '+str(figure[0])+' confirmed cases in '+county+' on '+date
            }
        )
        
def lambda_handler(event, context):
    # TODO implement
    logger.debug(event)
    return get_county_data(event)
