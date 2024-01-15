from kafka import KafkaProducer
import requests
from datetime import datetime
import time
import json

api_key = "f6f86bd33149ae349a5294a30d2cde23"


d = {
    "casablanca":[33.5945144,-7.6200284],
    "rabat":[34.02236,-6.8340222],
    "fès":[34.0346534,-5.0161926],
    "kenitra":[34.26457,-6.570169],
    "marrakech":[31.6258257,-7.9891608],
    "meknès":[33.8984131,-5.5321582],
    "smara":[26.7435827,-11.6645492],
    "salé":[34.044889,-6.814017],
    "tanger":[35.7696302,-5.8033522],
    "oujda":[34.677874,-1.929306],
    "agadir":[30.4205162,-9.5838532],
    "settat":[33.002397,-7.619867],
    "el jadida":[33.2336952,-8.5004248],
    "laâyoune,":[27.154512,-13.1953921],
    "khouribga":[32.8856482,-6.908798],
    "dakhla":[23.6940663,-15.9431274],
    "témara":[33.917166,-6.923804],
    "technopolis":[33.9877,-6.724],
    "nearshore":[33.5293,-7,6404],
    "agadir melloul":[30.2321967,-7.794225],
    "ain el aouda":[33.803077,-6.79413]
    }




p= KafkaProducer(bootstrap_servers='kafka:9092')




def create_table_from_res(res):
    d={
        "weather":{},
        "date":None
    }
    d['weather']['longitude']=res['coord']['lon']
    d['weather']['latitude']=res['coord']['lat']
    d['weather']['temperature'] = res['main']['temp']
    d['weather']['feels_like']= res['main']['feels_like']
    d['weather']['pressure'] = res['main']['pressure']
    d['weather']['humidity'] = res['main']['humidity']
    try :
        d['weather']['rain_1h']=res['rain']['1h']
    except:
        d['weather']['rain_1h']=None
    try:
        d['weather']['clouds'] = res['clouds']['all']
    except:
        d['weather']['clouds']=None
    try:
        d['weather']['snow']=res['snow']['1h']
    except:
        d['weather']['snow']=None
    d['date'] = datetime.utcfromtimestamp(res['dt']).strftime('%Y-%m-%d %H:%M:%S')
    return d

while True : 
    for key , pos in d.items():
        lat = pos[0]
        lon = pos[1]
        uri = f'https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}'
        r = requests.get(uri)
        print(r.json())
        document = create_table_from_res(r.json())
        document_bytes = json.dumps(document).encode('utf-8')
        p.send('weather_topic_2',value=document_bytes )
        p.flush()
        time.sleep(30)
    

