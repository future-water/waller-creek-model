import numpy as np
import pandas as pd

m_per_ft = 0.3048
ft_per_mi = 5280.
in_per_ft = 12.
s_per_min = 60
ns_per_s = 1e9

sample_interval = 30

def scs_composite_CN(CN_C, A_Imp):
    # Convert CN to composite CN given percent impervious area
    m = (99 - CN_C) / 100
    b = CN_C
    y = m * A_Imp + b
    return y


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


def precip_data(LCRA, start_date, end_date):
    LCRA = LCRA.loc[start_date:end_date]

    # Compute dt for each time bin
    LCRA['dt__s'] = np.roll(pd.Series(LCRA.index).diff(1).dt.seconds.values, -1)
    # Compute precipitation rate from total inches and dt
    LCRA['precip_rate__in_per_s'] = LCRA['precip_tot__in'] / LCRA['dt__s']

    # Compute inches of precipitation for desired sample interval
    precip__in = sample_interval * LCRA['precip_rate__in_per_s'].resample(f'{sample_interval}s').mean().interpolate()
    return precip__in


def scs_excess_precipitation(precip__in, CN,P_now, decay_function=lambda x: 0.):
    # Set up SCS parameters
    P = precip__in.values
    S = 1000 / CN -10 #potential maximum abstraction

    
    # Compute cumulative excess precipitation
    Pes = []
    Pes_inc = []
    Pe_prev = 0.
    P_now = P_now
    n = len(P)
    
    for t in range(n):
        Ia = 0.2 * S #inital abstraction
        Pt = P[t] #precipitaton at time t
     
        P_now = Pt + P_now  #accumulated precipitation
        loss = decay_function(P_now)
        if P_now - loss <= 0.:
            loss = 0.
        P_now = P_now - loss  ## I used this code for scs continous but still not fit well.
        
        if P_now <= Ia:
            Pe = 0.
        else:        
            Pe = (P_now - 0.2 * S)**2 / (P_now + 0.8 * S) #excess precipitation
        
        Pe_inc = Pe - Pe_prev + loss
        Pes.append(Pe)
        Pes_inc.append(Pe_inc)
        Pe_prev = Pe
        

    # Assign time index to excess precipitation output
    excess_precip_cum__in = pd.Series(Pes, index=precip__in.index)
    excess_precip__in = pd.Series(Pes_inc, index=precip__in.index)
    return excess_precip_cum__in, excess_precip__in, P_now 