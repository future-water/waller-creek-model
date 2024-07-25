import time
import schedule
from datetime import datetime, timedelta
from influxdb import InfluxDBClient
from pytz import timezone

import numpy as np
import pandas as pd
from pipedream_solver.hydraulics import SuperLink
from pipedream_solver.simulation import Simulation


from pipedream_solver.nutils import interpolate_sample

# Define runoff functions
from hydrology import scs_composite_CN, scs_excess_precipitation, scs_uh_runoff, precip_data, scs_excess_precipitation



# definition of query to call influxdb data
def recall_sensor_depth(client, field, measurement, tags, pagesize=10000):
    current_time = datetime.now(timezone('UTC'))

    published_gmt_minus=current_time-timedelta(hours = 24)
    now=current_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    now_minus=published_gmt_minus.strftime("%Y-%m-%dT%H:%M:%SZ")

    
    #select the length of node to bottom  
    length_ntb=ntb_df.at[tags['node_id'],'depth']
    
    collect = []
    times = []
    values = []
    q = True
    pagenum = 0
    # Single quotes around tags might not always work
    tag_str = ' AND '.join(["{key}='{value}'".format(key=key, value=value) for key, value
                            in tags.items()])
    
    
    while q:
        q = client.query(("SELECT {field} FROM {measurement} WHERE {tags} AND time >= '"+now_minus+"' AND time <= '"+now+"'"
                          "LIMIT {pagesize} OFFSET {page}")
                          .format(field=field, measurement=measurement, tags=tag_str,
                                  pagesize=pagesize, page=pagenum*pagesize))
        if q:
            collect.append(q[measurement])
           
        pagenum += 1
    for resultset in collect:
        for reading in resultset:
            dt_gmt=pd.Timestamp(reading['time'],tz='America/Chicago')
            times.append(dt_gmt)
            values.append((length_ntb-reading[field])*0.001)
            
    s = pd.Series(values, index=times)
    s.index = pd.to_datetime(s.index)
    
    
    drop_index=s[s<=0].index
    s=s.drop(index=drop_index)


    return s




#set client and inital parameter
def recall_forecast_rainIntensity():
    #match the timezone
    current_time = datetime.now(timezone('UTC'))
    
    published_gmt_minus=current_time-timedelta(hours = 24)
    now=current_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    now_minus=published_gmt_minus.strftime("%Y-%m-%dT%H:%M:%SZ")


    # collect precipiation probability
    now_result=client.query("SELECT rainIntensity FROM weather_forecast WHERE location ='30.2871667, -97.7341111' AND time >= '"+now_minus+"' AND time <= '"+now+"'")
    delta=datetime.strptime(now,'%Y-%m-%dT%H:%M:%SZ')-datetime.strptime(now_minus,'%Y-%m-%dT%H:%M:%SZ')
    delta=delta.total_seconds()
    return now_result, delta


# Compute runoff into each superjunction
def Model_initialization(precip__in,subbasins,P_now,decay_function=lambda x: 0.):
    keepGoing=True
    while keepGoing:
        try:
            Q_in = {}
            CNs = []
            # Manual edits to hydrology params
            lag_time_adjust_ratio = 1.0
            CN_adjust_ratio = 1.0


            # For each subbasin...
            for i in range(len(subbasins)):
                Pnow = P_now
                # Load subbasin parameters
                CN_C = subbasins['Curve Number'][i]
                A_Imp = subbasins['Impervious Percent'][i]
                area__sq_mi = subbasins['Area (mi2)'][i]
                downstream = subbasins['Downstream'][i]
                lag_time__min = subbasins['Lag Time'][i] * lag_time_adjust_ratio

                # Compute composite curve number
                CN = scs_composite_CN(CN_C, A_Imp) * CN_adjust_ratio
                CN = min(CN, 98)

                # Compute excess precipitation
                excess_precip_cum__in, excess_precip__in, P_now = scs_excess_precipitation(precip__in, CN,Pnow,decay_function=decay_function)

                # Compute runoff using unit hydrograph
                runoff__cms = scs_uh_runoff(excess_precip__in, uh__dimless, area__sq_mi, sample_interval,lag_time__min)
                Q_in[downstream] = runoff__cms

            # Format flow input as DataFrame
            Q_in = pd.DataFrame.from_dict(Q_in)
            # Add flow input for missing sites
            Q_in['UPSTREAM_WALLER'] = Q_in['R_WLR01']
            Q_in['UPSTREAM_HEMPHILL'] = Q_in['R_HEM01']
            Q_in[[name for name in superjunctions['name'] if not name in Q_in.columns]] = 0.
            # Ensure flow input is only for superjunctions specified in table
            Q_in = Q_in[superjunctions['name'].tolist()]
            # Remove NaN values
            Q_in = Q_in.fillna(0.)
            # Copy flow input with original timestamps
            Q_in_orig = Q_in.copy()
            # Convert flow input index to integer index starting with zero
            Q_in.index = Q_in.index.astype(int) / 1e9
            Q_in.index -= Q_in.index.min()
            keepGoing=False
        
        except ValueError:
            keepGoing=True
        except KeyError :
            keepGoing=True
        except AssertionError:
            keepGoing=True


    return excess_precip__in,Q_in,P_now

# Run simulation with KF
def Model_simulation(excess_precip__in,Q_in,dt,superlinks,superjunctions,measurements,load_data,stop_kf_time,delta):

    keepGoing=True
    while keepGoing:
        try:

            superlink = SuperLink(superlinks, superjunctions,internal_links=30, mobile_elements=True)


            H_j = []
            h_Ik = []
            Q_uk = []
            Q_dk = []
            residuals = []
            scores = []

            # Set constant timestep (in seconds)

            # Add constant baseflow
            baseflow = 0.35e-3 * np.ones(superlink._h_Ik.size)

            # Create simulation context manager
            with Simulation(superlink, Q_in=Q_in, Qcov=Qcov, Rcov=Rcov,
                        C=C_kal, H=H_kal, interpolation_method='nearest') as simulation:
                # While simulation time has not expired...
                simulation.model.load_state(load_data)
                simulation.t_end=delta
                while simulation.t <= simulation.t_end:
                    if simulation.t == 3600:
                        final_data=simulation.model.states
                    # Step model forward in time
                    simulation.step(dt=dt, num_iter=8, Q_Ik=baseflow)
                    # Get measured value
                    cond_kf = simulation.t < stop_kf_time
                    if cond_kf:
                        next_measurement = interpolate_sample(simulation.t,
                                                          measurements.index.values,
                                                          measurements.values,
                                                          method=0)
                        # Apply Kalman filter with measured value
                        H = H_kal
                        C = C_kal
                        Z_next = next_measurement
                        P_x_k_k = simulation.P_x_k_k
                        A_1, A_2, b = simulation.model._semi_implicit_system(_dt=dt)
                        I = np.eye(A_1.shape[0])
                        y_k1_k = b
                        A_1_inv = np.linalg.inv(A_1)
                        H_1 = H @ A_1_inv

                        residual = (Z_next - H_1 @ y_k1_k)
                        residuals.append(residual)

                        cond = residual**2 > 0.5


                        if (cond).any():
                            H_mod = H[~cond]
                            H_1 = H_mod @ A_1_inv
                            Rcov_mod = Rcov[~cond][:, ~cond]
                            Z_next = Z_next[~cond]
                        else:
                            H_mod = H
                            Rcov_mod = Rcov

                        P_y_k1_k = A_2 @ P_x_k_k @ A_2.T + C @ Qcov @ C.T
                        L_y_k1 = P_y_k1_k @ H_1.T @ np.linalg.inv((H_1 @ P_y_k1_k @ H_1.T) + Rcov_mod)
                        P_y_k1_k1 = (I - L_y_k1 @ H_1) @ P_y_k1_k
                        b_hat = y_k1_k + L_y_k1 @ (Z_next - H_1 @ y_k1_k)
                        P_x_k1_k1 = A_1_inv @ P_y_k1_k1 @ A_1_inv.T
                        #if score < 1e-7:
                        simulation.P_x_k_k = P_x_k1_k1
                        simulation.model.b = b_hat
                        simulation.model.iter_count -= 1
                        simulation.model.t -= dt
                        simulation.model._solve_step(dt=dt)

                    #simulation.kalman_filter(next_measurement, dt=dt)
                    simulation.model.reposition_junctions()
                    # Print progress bar
                    simulation.print_progress()
                    # Save states
                    H_j.append(simulation.model.H_j.copy())
                    h_Ik.append(simulation.model.h_Ik.copy())
                    Q_uk.append(simulation.model.Q_uk.copy())
                    Q_dk.append(simulation.model.Q_dk.copy())

            time_index = pd.date_range(start=excess_precip__in.index.min(),
                                               periods=len(H_j),
                                               freq=f'{dt}s')

            # Convert saved states to dataframes
            H_j = pd.DataFrame(np.vstack(H_j), index=time_index, columns=superjunctions['name'])
            h_Ik = pd.DataFrame(np.vstack(h_Ik), index=time_index)
            Q_uk = pd.DataFrame(np.vstack(Q_uk), index=time_index, columns=superlinks['name'])
            Q_dk = pd.DataFrame(np.vstack(Q_dk), index=time_index, columns=superlinks['name'])


            # Compute depth
            h_j = H_j - simulation.model._z_inv_j

            keepGoing=False

        except ValueError:
            keepGoing=True
        except KeyError :
            keepGoing=True
        except AssertionError:
            keepGoing=True

    return H_j,h_Ik,Q_uk,Q_dk,h_j,final_data

def save_simulation_result(h_j, Q_uk, client3):
    h_j_selected=h_j[['J_WLR16_32ND','J_WLR18_COMB_HEMP','J_WLR19_23RD','J_WLR20_TRINITY']]
    Q_uk_selected=Q_uk[['R_WLR15','R_WLR16','R_WLR17']]
    for n in range(0, len(h_j_selected), 1000):
        dataset1 = h_j_selected[n:n + 1000]
        dataset2 = Q_uk_selected[n:n + 1000]
        dataset_list = [dataset1, dataset2]
        measurement = ['depth', 'flow']
        for num in range(len(dataset_list)):
            dataset = dataset_list[num]
            _measurement = measurement[num]
            for loc in dataset.columns:
                json_body = []
                _location = loc
                for i in range(0,len(dataset),180):
                    _time = dataset[_location].index[i]
                    _value = dataset[_location][i]
                    json_data = {
                                    "measurement": _measurement,
                                    "tags": {
                                        "location": _location,
                                    },
                                    "time": _time,
                                    "fields": {
                                        "value": _value,
                                    }
                                },
                    json_body.extend(json_data)
                client3.write_points(json_body)


# client needs to be set
client = InfluxDBClient(host='',username='',password='',database='' )
client3 = InfluxDBClient(host='',username='',password='' ,database='')
client2 = InfluxDBClient(host='', username='',password='',database='')

# Load sensor node information
# set up the lists(node_id and length between node and bottom)
node_id_list=['Bridge1','Bridge2','Bridge3','Bridge4','Bridge5']
length_ntb_list =[3911.6, 3175, 4927.6, 4978.4, 3556]
ntb_df=pd.DataFrame(length_ntb_list, columns=['depth'])
ntb_df=ntb_df.set_index([node_id_list])


# Load pipedream model information
superjunctions = pd.read_csv('/home/ubuntu/data/waller_superjunctions.csv', index_col=0)
superlinks = pd.read_csv('/home/ubuntu/data/waller_creek_superlinks.csv', index_col=0)
subbasins = pd.read_csv('/home/ubuntu/data/Waller_HMS_model_data.csv',index_col=0)


# Specify simulation parameters
# Unit conversions
m_per_ft = 0.3048
ft_per_mi = 5280.
in_per_ft = 12.
s_per_min = 60
ns_per_s = 1e9

# Manual edits to model
superlinks.loc[20, ['g2', 'g3']] = 5.6, 6.3
superlinks.loc[21, ['g2', 'g3']] = 3.7, 3.977146367746631
superlinks.loc[22, ['g2', 'g3']] = 0., 7.777146367746631
superlinks.loc[23, ['g2', 'g3']] = 0., 8.3714367746631
superlinks['C_uk'] = 0.
superlinks['C_dk'] = 0.
superjunctions.loc[16, ['z_inv']]=147.9

# Simulation params
sample_interval = 30
site_junction_name = 'J_WLR18_COMB_HEMP'
site_link_name = 'R_WLR16'
site_junction_index = 14
site_link_index = 21

# Load unit hydrograph
# Load dimensionless unit hydrograph
uh__dimless = pd.read_csv('/home/ubuntu/data/unit_hydrograph.csv')
# Drop unnecessary columns and rows
uh__dimless = uh__dimless[['time ratios', 'discharge ratios']].dropna()
dt =10


# Set up Kalman filtering parameters
n = len(superjunctions)
p = n
m = 4

process_std_dev = 1e-2
measurement_std_dev = 2e-4

H_kal = np.zeros((m, n))
H_kal[0, 13] = 1.
H_kal[1, 14] = 1.
H_kal[2, 15] = 1.
H_kal[3, 16] = 1.
Qcov = (process_std_dev**2)*np.eye(p)
Rcov = (measurement_std_dev**2)*np.eye(m)

C_kal = np.zeros((n, p))
C_kal[np.arange(n), np.arange(p)] = 1.


def Activate_simulation(final_data,P_now):
    #Recall precipitation data
    now_result,delta=recall_forecast_rainIntensity()
            
    forecast_precip=np.array([],dtype='f')
    forecast_time=np.array([],dtype='str')
    for reading in now_result:
        for data in reading:
            forecast_precip=np.append (forecast_precip,data['rainIntensity'])
            forecast_time=np.append (forecast_time,data['time'])

    
    forecast_time = pd.to_datetime(forecast_time, utc=True)
    # Define the Texas time zone
    texas_tz = timezone('America/Chicago')  # 'America/Chicago' represents the Central Time Zone
    # Convert the datetime index from UTC to the Texas time zone
    forecast_time = forecast_time.tz_convert(texas_tz)
    # Create a DataFrame with forecast_precip using forecast_time as the index
    forecast_precip = pd.DataFrame(forecast_precip/60, index=forecast_time, columns=['precip_tot__in'])

    forecast_precip['dt__s'] = np.roll(pd.Series(forecast_precip.index).diff(1).dt.seconds.values, -1)
    # Compute precipitation rate from total inches and dt
    forecast_precip['precip_rate__in_per_s'] = forecast_precip['precip_tot__in'] / forecast_precip['dt__s']
    # Compute inches of precipitation for desired sample interval
    precip__in_avg = sample_interval * forecast_precip['precip_rate__in_per_s'].resample(f'{sample_interval}s').mean().interpolate()
    precip__in=precip__in_avg
    
    
    #Recall Sensor data
    bridge_2 = recall_sensor_depth(client2, 'value', 'depth', {'node_id' : 'Bridge2'})
    bridge_3 = recall_sensor_depth(client2, 'value', 'depth', {'node_id' : 'Bridge3'})
    bridge_4 = recall_sensor_depth(client2, 'value', 'depth', {'node_id' : 'Bridge4'})
    bridge_5 = recall_sensor_depth(client2, 'value', 'depth', {'node_id' : 'Bridge5'})
    
    #run Model 
    stop_kf_time=delta
    if precip__in.empty != True:
        excess_precip__in,Q_in,P_now=Model_initialization(precip__in,subbasins,P_now,decay_function=lambda x: (1 - 0.9974)*x)

        measurements = pd.concat([bridge_2.resample('5min').mean().interpolate(method='nearest'),
                                  bridge_5.resample('5min').mean().interpolate(method='nearest'),
                              bridge_3.resample('5min').mean().interpolate(method='nearest'),
                              bridge_4.resample('5min').mean().interpolate(method='nearest')
                                 ], axis=1).interpolate()
        measurements = measurements.fillna(method='backfill')
        measurements = measurements + superjunctions.loc[[13, 14, 15, 16], 'z_inv'].values
        measurements.index = measurements.index - precip__in.index.min()
        measurements.index = measurements.index.astype(int) / 1e9
        H_j,h_Ik,Q_uk,Q_dk,h_j,final_data=Model_simulation(excess_precip__in,Q_in,dt,superlinks,superjunctions,measurements,final_data,stop_kf_time,delta)
        final_data['t']=0
        
        current_time = datetime.now(timezone('America/Chicago'))
        current_minus1h=current_time-timedelta(hours = 1)
        h_j_print=h_j[current_minus1h:]
        Q_uk_print=Q_uk[current_minus1h:]
        save_simulation_result(h_j_print, Q_uk_print, client3)
    return final_data,P_now


def main():
    final_data={}
    P_now=0
    schedule.every().hour.at(":20").do(Activate_simulation,final_data,P_now)
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__=="__main__":
    main()
