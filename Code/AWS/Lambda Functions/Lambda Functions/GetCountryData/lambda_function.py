import json
import logging
import pymysql
from datetime import datetime as dt
import datetime

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
    

def isvalid_country(country):
    all_countries = ['afghanistan', 'albania', 'algeria', 'andorra', 'angola', 'antigua_and_barbuda', 'argentina', 'armenia', 
    'australia', 'austria', 'azerbaijan', 'bahamas', 'bahrain', 'bangladesh', 'barbados', 'belarus', 'belgium', 'belize', 
    'benin', 'bhutan', 'bolivia', 'bosnia_and_herzegovina', 'botswana', 'brazil', 'brunei_darussalam', 'bulgaria', 
    'burkina_faso', 'burundi', 'cambodia', 'cameroon', 'canada', 'cape_verde', 'central_african_republic', 'chad', 
    'chile', 'china', 'colombia', 'comoros', 'congo_brazzaville', 'congo_kinshasa', 'costa_rica', 'croatia', 'cuba', 
    'cyprus', 'czech_republic', 'côte_divoire', 'denmark', 'djibouti', 'dominica', 'dominican_republic', 'ecuador', 
    'egypt', 'el_salvador', 'equatorial_guinea', 'eritrea', 'estonia', 'ethiopia', 'fiji', 'finland', 'france', 'gabon', 
    'gambia', 'georgia', 'germany', 'ghana', 'greece', 'grenada', 'guatemala', 'guinea', 'guineabissau', 'guyana', 'haiti', 
    'holy_see_vatican_city_state', 'honduras', 'hungary', 'iceland', 'india', 'indonesia', 'iran_islamic_republic_of', 'iraq', 
    'ireland', 'israel', 'italy', 'jamaica', 'japan', 'jordan', 'kazakhstan', 'kenya', 'korea_south', 'kuwait', 'kyrgyzstan', 
    'lao_pdr', 'latvia', 'lebanon', 'lesotho', 'liberia', 'libya', 'liechtenstein', 'lithuania', 'luxembourg', 'macedonia_republic_of', 
    'madagascar', 'malawi', 'malaysia', 'maldives', 'mali', 'malta', 'mauritania', 'mauritius', 'mexico', 'moldova', 'monaco', 
    'mongolia', 'montenegro', 'morocco', 'mozambique', 'myanmar', 'namibia', 'nepal', 'netherlands', 'new_zealand', 'nicaragua',
    'niger', 'nigeria', 'norway', 'oman', 'pakistan', 'palestinian_territory', 'panama', 'papua_new_guinea', 'paraguay', 'peru', 
    'philippines', 'poland', 'portugal', 'qatar', 'republic_of_kosovo', 'romania', 'russian_federation', 'rwanda', 
    'saint_kitts_and_nevis', 'saint_lucia', 'saint_vincent_and_grenadines', 'san_marino', 'sao_tome_and_principe', 
    'saudi_arabia', 'senegal', 'serbia', 'seychelles', 'sierra_leone', 'singapore', 'slovakia', 'slovenia', 'somalia', 
    'south_africa', 'south_sudan', 'spain', 'sri_lanka', 'sudan', 'suriname', 'swaziland', 'sweden', 'switzerland', 
    'syrian_arab_republic_syria', 'taiwan_republic_of_china', 'tajikistan', 'tanzania_united_republic_of', 'thailand', 
    'timorleste', 'togo', 'trinidad_and_tobago', 'tunisia', 'turkey', 'uganda', 'ukraine', 'united_arab_emirates', 
    'united_kingdom', 'united_states_of_america', 'uruguay', 'uzbekistan', 'venezuela_bolivarian_republic', 'viet_nam', 
    'western_sahara', 'yemen', 'zambia', 'zimbabwe']
    return country.lower() in all_countries

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

def get_country_data(intent_request):
    intent_name = intent_request['currentIntent']['name']
    country_name = intent_request['currentIntent']['slots']['Country'].replace(" ", "_")
    logger.debug(country_name)
    logger.debug(isvalid_country(country_name))
    if country_name and not isvalid_country(country_name):
        validation_result = build_validation_result(
            False,
            'Country',
            'The Country {} is invalid, Can you try a different Country'.format(country_name)
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
        country = intent_request['currentIntent']['slots']['Country'].replace(" ", "_")
        typeofCase = intent_request['currentIntent']['slots']['TypeOfCases']
        date = intent_request['currentIntent']['slots']['Date']
        
        #Check the latest date available in the database
        date_query = "Select Max(Date) from " +typeofCase+ "cases"
        latest_date_db = get_db_cursor(date_query).fetchall()[0]
        logger.debug(latest_date_db[0])
        if dt.strptime(date, '%Y-%m-%d').date() <= latest_date_db[0]:
            query = "Select " +country+ " from " +typeofCase+ "cases where Date='"+date+"'"
            logger.debug(query)
            enquiry_figure = get_db_cursor(query).fetchall()[0]
            #Get New Confirmed Cases 
            previous_date =dt.strftime(dt.strptime(date, '%Y-%m-%d') - datetime.timedelta(days=1), '%Y-%m-%d')
            query = "Select " + country + " from " +typeofCase+ "cases where Date='"+previous_date+"'"
            new_cases_today = get_db_cursor(query).fetchall()[0]
            today_value = str(abs(enquiry_figure[0] - new_cases_today[0]))
            return close(
                {},
                'Fulfilled',
                {
                    'contentType': 'PlainText',
                    'content': 'There were '+str(enquiry_figure[0])+' of '+typeofCase+' Cases in '+country.replace("_", " ")+' on '+date+' with '+today_value+' new '+typeofCase+' case(s) recorded on '+date+'. Say Hi to start again'
                }
            )
        else:
            #Elicit SLot
            validation_result = build_validation_result(
            False,
            'Date',
            'The Date {} is not yet present in our database, try entering a date on or before {} here'.format(date, latest_date_db[0])
            )
            logger.debug(validation_result)
            intent_request['currentIntent']['slots'][validation_result['violatedSlot']] = None
            return elicit_slot(
                {},
                intent_request['currentIntent']['name'],
                intent_request['currentIntent']['slots'],
                validation_result['violatedSlot'],
                validation_result['message']
            )
def lambda_handler(event, context):
    # TODO implement
    logger.debug(event)
    return get_country_data(event)
