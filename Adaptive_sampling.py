
import time
import schedule
from datetime import datetime, timedelta
from influxdb import InfluxDBClient
from pytz import timezone

#inital setting
client = InfluxDBClient(host='ec2-13-58-107-29.us-east-2.compute.amazonaws.com',username='ewre_student',password='happylife!',database='tomorrow_api' )
client2 = InfluxDBClient(host='ec2-3-91-156-8.compute-1.amazonaws.com', port='8086', username='generic_node',
                            password='GlareShellTwilight')


def recall_precipProbability():
    # match the timezone
    current_time = datetime.now()
    utc = timezone('UTC')
    central = timezone('US/Central')
    published_gmt = current_time.replace(tzinfo=central)
    published_cst = published_gmt.astimezone(utc)
    published_cst_plus1h = published_cst + timedelta(hours=1)

    # set time scale (now : now+1h)
    now = published_cst.strftime("%Y-%m-%dT%H:%M:%SZ")
    now_plus1 = published_cst_plus1h.strftime("%Y-%m-%dT%H:%M:%SZ")

    # collect precipiation probability
    now_result = client.query(
        "SELECT precipitationProbability FROM weather_forecast WHERE location ='30.2871667, -97.7341111' AND time >= '" + now + "' AND time <= '" + now_plus1 + "'")
    return now_result


def cal_sum_precipProbability(result):
    # calculate the sum of precipitation probability
    sum_value = 0
    for reading in result:
        for data in reading:
            sum_value += data['precipitationProbability']

    return sum_value


# set state for sampling frequecy
def set_precip_state(sum_value):
    if sum_value == 0:
        state = 'Normal'
    else:
        state = 'Rainy'

    return state


def Adaptive_sampling():
    now_result = recall_precipProbability()
    now_result_sum = cal_sum_precipProbability(now_result)
    now_state = set_precip_state(now_result_sum)

    if now_state == 'Rainy':
        collecting_time = 15
    else:
        collecting_time = 30

    client2.write_points(['Adaptive_sampling_time,node_id=Bridge2 value=' + str(collecting_time)], database='RAW',
                         protocol='line')
    client2.write_points(['Adaptive_sampling_time,node_id=Bridge5 value=' + str(collecting_time)], database='RAW',
                         protocol='line')
    client2.write_points(['Adaptive_sampling_time,node_id=Bridge3 value=' + str(collecting_time)], database='RAW',
                         protocol='line')
    client2.write_points(['Adaptive_sampling_time,node_id=Bridge4 value=' + str(collecting_time)], database='RAW',
                         protocol='line')
    client2.write_points(['Adaptive_sampling_time,node_id=Bridge1 value=' + str(collecting_time)], database='RAW',
                         protocol='line')


def main():
    # Adaptive crawling -pretest for collecting date
    schedule.every().hour.at("00:10").do(Adaptive_sampling)
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__=="__main__":
    main()



