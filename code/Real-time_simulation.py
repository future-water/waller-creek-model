import time
import schedule
from datetime import datetime, timedelta
from influxdb import InfluxDBClient
from pytz import timezone

import numpy as np
import pandas as pd
from pipedream_solver.hydraulics import SuperLink
from pipedream_solver.simulation import Simulation

#set client and inital parameter
def recall_forecast_rainIntensity():
    #match the timezone
    current_time = datetime.now(timezone('UTC'))
    
    published_gmt_plus1h=current_time+timedelta(hours = 1)
    published_gmt_minus=current_time-timedelta(hours = 23)
    now_plus1=published_gmt_plus1h.strftime("%Y-%m-%dT%H:%M:%SZ")
    now_minus=published_gmt_minus.strftime("%Y-%m-%dT%H:%M:%SZ")


    # collect precipiation probability
    now_result=client.query("SELECT rainIntensity FROM weather_forecast WHERE location ='30.2871667, -97.7341111' AND time >= '"+now_minus+"' AND time <= '"+now_plus1+"'")
    delta=datetime.strptime(now_plus1,'%Y-%m-%dT%H:%M:%SZ')-datetime.strptime(now_minus,'%Y-%m-%dT%H:%M:%SZ')
    delta=delta.total_seconds()
    return now_result, delta

# Define runoff functions
def scs_composite_CN(CN_C, A_Imp):
    # Convert CN to composite CN given percent impervious area
    m = (99 - CN_C) / 100
    b = CN_C
    y = m * A_Imp + b
    return y


def scs_excess_precipitation(precip__in, CN):
    # Set up SCS parameters
    P = precip__in.values
    S = 1000 / CN - 10
    Ia = 0.2 * S
    # Compute cumulative excess precipitation
    Pes = []
    P_now = 0
    n = len(P)
    for t in range(n):
        Pt = P[t]
        P_now = Pt + P_now
        if P_now <= Ia:
            Pe = 0.
        else:
            Pe = (P_now - 0.2 * S)**2 / (P_now + 0.8 * S)
        Pes.append(Pe)
    # Assign time index to excess precipitation output
    excess_precip_cum__in = pd.Series(Pes, index=precip__in.index)
    return excess_precip_cum__in

def scs_uh_runoff(excess_precip__in, uh__dimless, area__sq_mi, sample_interval,lag_time__min):

    # Give unit hydrograph a time dimension corresponding to lag time
    uh = uh__dimless.copy()
    uh_time = uh['time ratios'] * lag_time__min * s_per_min * ns_per_s
    uh_time = pd.to_datetime(uh_time.rename('time'))
    uh = uh.set_index(uh_time)['discharge ratios']
    # Resample unit hydrograph to desired time interval
    uh = uh.resample(f'{sample_interval}s').mean().interpolate()
    uh = uh.values
    # Scale unit hydrograph to have area of unity
    uh_scaled = uh / uh.sum()

    # Convolve excess precipitation and unit hydrograph
    conv = np.convolve(excess_precip__in.values, uh_scaled)
    # Convert to outflow
    area__sq_ft = area__sq_mi * (ft_per_mi)**2
    conv__ft = conv / in_per_ft
    runoff__cfs = (conv__ft * area__sq_ft) / sample_interval
    runoff__cms = runoff__cfs * (m_per_ft)**3
    time_index = pd.date_range(start=excess_precip__in.index.min(),
                               periods=runoff__cms.size,
                               freq=f'{sample_interval}s')
    # Return runoff in cubic meters per second
    runoff__cms = pd.Series(runoff__cms, index=time_index)
    return runoff__cms

# Compute runoff into each superjunction
def Model_initialization(precip__in,subbasins):
    Q_in = {}
    CNs = []
    # Manual edits to hydrology params
    lag_time_adjust_ratio = 1.0
    CN_adjust_ratio = 1.0


    # For each subbasin...
    for i in range(len(subbasins)):
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
        excess_precip_cum__in = scs_excess_precipitation(precip__in, CN)
        excess_precip__in = -excess_precip_cum__in.diff(-1).fillna(0.)

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
    return excess_precip__in,Q_in


# Run simulation without KF
def Model_simulation(excess_precip__in,Q_in,dt,superlinks,superjunctions,delta,load_data):
    load_data=load_data
    keepGoing=True
    while keepGoing:
        try:
            superlink = SuperLink(superlinks, superjunctions,internal_links=30,mobile_elements=True)

            H_j = []
            h_Ik = []
            Q_uk = []
            Q_dk = []

            # Set constant timestep (in seconds)

            # Add constant baseflow
            baseflow = 0.35e-3 * np.ones(superlink._h_Ik.size)

            # Create simulation context manager
            with Simulation(superlink, Q_in=Q_in) as simulation:
                # While simulation time has not expired...
                simulation.model.load_state(load_data)
                simulation.t_end=delta
                while simulation.t <= simulation.t_end:
                    if simulation.t == 3600:
                        final_data=simulation.model.states

                    # Step model forward in time
                    simulation.step(dt=dt, num_iter=8, Q_Ik=baseflow)
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
            #final_data=simulation.model.states

            keepGoing=False
        except ValueError:
            keepGoing=True
        except KeyError:
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


#####client need to be set. 
client = InfluxDBClient(host='',username='',password='',database='' )
client3 = InfluxDBClient(host='',username='',password='' ,database='')

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



def Activate_simulation(final_data): 
    now_result,delta=recall_forecast_rainIntensity()
            
    forecast_precip=np.array([],dtype='f')
    forecast_time=np.array([],dtype='str')
    for reading in now_result:
        for data in reading:
            forecast_precip=np.append (forecast_precip,data['rainIntensity'])
            forecast_time=np.append (forecast_time,data['time'])

    
    forecast_time = pd.to_datetime(forecast_time, utc=True)
    # Create a DataFrame with forecast_precip using forecast_time as the index
    forecast_precip = pd.DataFrame(forecast_precip/60, index=forecast_time, columns=['precip_tot__in'])

    forecast_precip['dt__s'] = np.roll(pd.Series(forecast_precip.index).diff(1).dt.seconds.values, -1)
    # Compute precipitation rate from total inches and dt
    forecast_precip['precip_rate__in_per_s'] = forecast_precip['precip_tot__in'] / forecast_precip['dt__s']
    # Compute inches of precipitation for desired sample interval
    precip__in_avg = sample_interval * forecast_precip['precip_rate__in_per_s'].resample(f'{sample_interval}s').mean().interpolate()
    precip__in=precip__in_avg
    

    excess_precip__in,Q_in=Model_initialization(precip__in,subbasins)
    H_j,h_Ik,Q_uk,Q_dk,h_j,final_data=Model_simulation(excess_precip__in,Q_in,dt,superlinks,superjunctions,delta,final_data)
    current_time = datetime.now(timezone('America/Chicago'))
    plus1h=current_time+timedelta(hours = 1)
    h_j_print=h_j[current_time:plus1h]
    Q_uk_print=Q_uk[current_time:plus1h]
    save_simulation_result(h_j_print, Q_uk_print, client3)
    final_data['t']=0

    return final_data    


def main():
    final_data={}
    schedule.every().hour.at(":10").do(Activate_simulation,final_data)
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__=="__main__":
    main()
