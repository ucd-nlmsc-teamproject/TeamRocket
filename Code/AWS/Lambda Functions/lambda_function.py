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

def db_parameters():
    endpoint = 'database-1.cdnnok48tja6.us-east-1.rds.amazonaws.com'
    username = 'nippo1994'
    password = 'covidbot123'
    database_name = 'test_db'
    return endpoint, username, password, database_name
    

def get_db_cursor(typeofCase, query):
    connection = pymysql.connect(endpoint, user=username,
    passwd=password, db=database_name)
    endpoint, username, password, database_name = db_parameters()
    cursor = connection.cursor()
    cursor.execute(query)
    return cursor
    

def isvalid_country(country):
    all_countries = ['united states', 'afghanistan', 'albania', 'algeria', 'american samoa', 'andorra', 'angola', 'anguilla',
 'antarctica', 'antigua and barbuda', 'argentina', 'armenia', 'aruba', 'australia', 'austria', 'azerbaijan', 'bahamas',
 'bahrain', 'bangladesh', 'barbados', 'belarus', 'belgium', 'belize', 'benin', 'bermuda', 'bhutan', 'bolivia', 'bosnia and herzegowina',
 'botswana', 'bouvet island', 'brazil', 'brunei darussalam', 'bulgaria', 'burkina faso', 'burundi', 'cambodia', 'cameroon',
 'canada', 'cape verde', 'cayman islands', 'central african rep', 'chad', 'chile', 'china', 'christmas island', 'cocos islands', 'colombia',
 'comoros', 'congo', 'cook islands', 'costa rica', 'cote d`ivoire', 'croatia', 'cuba', 'cyprus', 'czech republic', 'denmark',
 'djibouti', 'dominica', 'dominican republic', 'east timor', 'ecuador', 'egypt', 'el salvador', 'equatorial guinea', 'eritrea',
 'estonia', 'ethiopia', 'falkland islands (malvinas)', 'faroe islands', 'fiji', 'finland', 'france', 'french guiana', 'french polynesia',
 'french s. territories', 'gabon', 'gambia', 'georgia', 'germany', 'ghana', 'gibraltar', 'greece', 'greenland', 'grenada', 'guadeloupe',
 'guam', 'guatemala', 'guinea', 'guinea-bissau', 'guyana', 'haiti', 'honduras', 'hong kong', 'hungary', 'iceland', 'india', 'indonesia',
 'iran', 'iraq', 'ireland', 'israel', 'italy', 'jamaica', 'japan', 'jordan', 'kazakhstan', 'kenya', 'kiribati', 'korea (north)', 'korea (south)',
 'kuwait', 'kyrgyzstan', 'laos', 'latvia', 'lebanon', 'lesotho', 'liberia', 'libya', 'liechtenstein', 'lithuania', 'luxembourg', 'macau',
 'macedonia', 'madagascar', 'malawi', 'malaysia', 'maldives', 'mali', 'malta', 'marshall islands', 'martinique', 'mauritania', 'mauritius', 'mayotte',
 'mexico', 'micronesia', 'moldova', 'monaco', 'mongolia', 'montserrat', 'morocco', 'mozambique', 'myanmar', 'namibia', 'nauru', 'nepal',
 'netherlands', 'netherlands antilles', 'new caledonia', 'new zealand', 'nicaragua', 'niger', 'nigeria', 'niue', 'norfolk island', 'northern mariana islands', 'norway',
 'oman', 'pakistan', 'palau', 'panama', 'papua new guinea', 'paraguay', 'peru', 'philippines', 'pitcairn', 'poland', 'portugal', 'puerto rico', 'qatar',
 'reunion', 'romania', 'russian federation', 'rwanda', 'saint kitts and nevis', 'saint lucia', 'st vincent/grenadines', 'samoa', 'san marino', 'sao tome',
 'saudi arabia', 'senegal', 'seychelles', 'sierra leone', 'singapore', 'slovakia', 'slovenia', 'solomon islands', 'somalia', 'south africa',
 'spain','sri lanka', 'st. helena', 'st.pierre', 'sudan', 'suriname', 'swaziland', 'sweden', 'switzerland', 'syrian arab republic', 'taiwan', 'tajikistan',
 'tanzania', 'thailand', 'togo', 'tokelau', 'tonga', 'trinidad and tobago', 'tunisia', 'turkey', 'turkmenistan', 'tuvalu', 'uganda',
 'ukraine', 'united arab emirates', 'united kingdom', 'uruguay', 'uzbekistan', 'vanuatu', 'vatican city state', 'venezuela', 'viet nam',
 'virgin islands (british)', 'virgin islands (u.s.)', 'western sahara', 'yemen', 'yugoslavia', 'zaire', 'zambia', 'zimbabwe']
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
    country_name = intent_request['currentIntent']['slots']['Country']
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
        #Code to fetch data from Database for the given country
        
def lambda_handler(event, context):
    # TODO implement
    logger.debug(event)
    return get_country_data(event)
