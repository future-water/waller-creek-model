
import requests
import time
import schedule
import json
from influxdb import InfluxDBClient
from datetime import datetime

client = InfluxDBClient(host='ec2-3-88-251-104.compute-1.amazonaws.com',username='generic_node',password='GlareShellTwilight',database='tomorrow_api' )

# Tomorrow.io API https://docs.tomorrow.io/reference/get-timelines
# use schedule module to do work

location=[('location', '30.3265250, -97.7199113'),#Relly pond
('location', '30.2871667, -97.7341111')]#bridge5

#Tomorrow.io API https://docs.tomorrow.io/reference/get-timelines
# use schedule module to do work


def crawling_job():
    for i in range(0,len(location)):
        collect_tomorrow_API_forecast_data(i)

def collect_tomorrow_API_forecast_data(i):
    set_location=location[i]
    #set_location=('location', '30.2871667, -97.7341111')

    params = (set_location,('units', 'imperial'),('timesteps','1m'),('startTime','now'),('endTime','nowPlus2h'),('apikey', 'VYLHerX2Zbk6nBvVo4VTkpGg2JFJTfw3'),)
    headers = {
    "Accept": "application/json",
    "Accept-Encoding": "gzip"}

    response = requests.get('https://api.tomorrow.io/v4/weather/forecast?', params=params, headers=headers)
    time.sleep(10)

    #change to dictionary format
    content=response.content
    dict_str = content.decode("ascii")
    my_data = json.loads(dict_str)
    
    try:
        now = datetime.now()
        current_time = now.strftime("%H:%M:%S")
        value=my_data['timelines']['minutely']
        _location=location[i][1]
        #_location=set_location[1]
        _measurement='weather_forecast'
        json_body = []
        for i in range(len(value)):
            current_time=value[i]['time']
            rain_probability=value[i]['values']['precipitationProbability']
            temp=value[i]['values']['temperature']
            rain_intensity=value[i]['values']['rainIntensity']

            json_data={
                            "measurement": _measurement,
                            "tags": {
                                "location":_location
                            },
                            "time": current_time,
                            "fields": {
                                "precipitationProbability": float(rain_probability),
                                "temperature": float(temp),
                                "rainIntensity": float(rain_intensity)

                            }
                        },
            json_body.extend(json_data)

        client.write_points(json_body)

    except KeyError as e:
        
        print('KeyError:', e)

def main():
    schedule.every().hour.at(":00").do(crawling_job)

    while True:

        # Checks whether a scheduled task
        # is pending to run or not
        schedule.run_pending()
        time.sleep(1)

if __name__=="__main__":
    main()


