#!/usr/bin/env python3
# -*- coding: utf-8 -*-
####################################
#                    ORIGINAL COPY.                   #
####################################


exec(open('imports.py').read())
from tqdm import tqdm

import multiprocessing

def calculate_me(year,levs_to_sel=[850,700,500,300,250]):
    
    
    """
    Calculate Moist Static Energy (MSE) in J/kg.

    Parameters:
    - temperature: Air temperature in Kelvin
    - specific_humidity: Specific humidity in kg/kg
    - pressure: Atmospheric pressure in Pa

    Returns:
    - mse: Moist Static Energy in J/kg
    -mde: Moist Dynamic Energy (vertical motion from 'gz')
    """
    save_path= 'Moist Energies/'
    mse_path = f'{save_path}Moist_Static_Energy'
    mde_path = f'{save_path}Moist_Dynamic_Energy'
    
    if os.path.isdir(save_path) ==False:
        os.mkdir(save_path)
    
    if os.path.isdir(mse_path)==False:
        os.mkdir(mse_path)
        
    if os.path.isdir(mde_path) == False:
        os.mkdir(mde_path)
        
    #loop through data range and select specific year duration and save 
    if year=='2000':
        ct=0
    else:
        ct = len(glob.glob(f'{mse_path}*.nc',recursive=True))
        
    mpath = '/global/cfs/projectdirs/m3522/cmip6/ERA5/e5.oper.an.pl/'
    print(f'{Color.BLUE}Loading datasets{Color.END}')
    ###### open specific humidity ########
    pths = glob.glob(f'{mpath}{year}*/*q.*' ,recursive=True)
    print(len(pths))
    print(f'{Color.YELLOW}Loading datasets for Q{Color.END}')
    specific_humidity = [xr.open_mfdataset(x).sel(level=levs_to_sel) for x in pths]
    specific_humidity = xr.concat(specific_humidity,dim='time')
    
    ###### open geopotential heights ######
    pths = glob.glob(f'{mpath}{year}*/*z.*' ,recursive=True)
    print(len(pths))
    print(f'{Color.YELLOW}Loading datasets for  Z{Color.END}')
    geopotential_height = [xr.open_mfdataset(x).sel(level=levs_to_sel) for x in pths]
    geopotential_height = xr.concat(geopotential_height,dim='time')
    
    
    ###### open temperature ######
    pths = glob.glob(f'{mpath}{year}*/*t.*' ,recursive=True)
    print(len(pths))
    print(f'{Color.YELLOW}Loading datasets for T{Color.END}')
    temperature = [xr.open_mfdataset(x).sel(level=levs_to_sel) for x in pths]
    temperature = xr.concat(temperature,dim='time')
    
    pressure = list(temperature.level.values)
    
    # Constants
    cp = 1004.  # Specific heat at constant pressure in J/(kg·K)
    g = 9.8  # Acceleration due to gravity in m/s²
    lav = 2.5e6  # Latent heat of vaporization in J/kg

    # Calculate potential temperature
    theta = [temperature.sel(level=lv) * (1000/lv)**(2. / 7.) for lv in tqdm(pressure,desc='Processing')]
    
    #concat and transpose theta data
    theta = xr.concat(theta,dim='level')
    theta = theta.transpose('time','level','latitude','longitude')
    
    # Calculate equivalent potential temperature (EPT)
    thetha_e = [theta.sel(level=lv) **((lav * specific_humidity.sel(level=lv)) / (cp * temperature.sel(level=lv))) for lv in tqdm(pressure,desc='Processing Theta')]

    p1 = (lav*specific_humidity)               #SH part 
    p2 = (cp*temperature)                        #Temp part 
    p3 = (g*geopotential_height)
    
    p4 = xr.Dataset()                                 #create dataset to hold EPT
    p4['theta_e']=xr.DataArray(np.multiply(theta['T'].values , np.exp(np.divide(p1['Q'].values,p2['T']).values)),dims=['time','level','latitude','longitude'])

    
    # Reassign dimensions to EPT
    p4['longitude'] = p1['longitude']
    p4['latitude'] = p1['latitude']
    p4['time'] = p1['time']

    # Calculate Moist Static Energy  (MSE)
    MSE = xr.Dataset()
    MSE['mse'] = xr.DataArray(p1['Q'].values + p2['T'].values + p4['theta_e'].values, dims=['time','level','latitude','longitude']).chunk({'time':1,'level':1})

    #Reassign dimensions 
    MSE['time'] = p1.time
    MSE['latitude'] = p1.latitude
    MSE['longitude'] = p1.longitude
    
    # Calculate Moist Dynamic Energy (MDE)
    #mde = cp * temperature + g * geopotential_height + lv * specific_humidity
    
    MDE = xr.Dataset()
    MDE['mde'] = xr.DataArray(p1['Q'].values + p2['T'].values + p3['Z'].values, dims=['time','level','latitude','longitude'] ).chunk({'time':1,'level':1})
    
    #Reassign dimensions 
    MDE['time'] = p1.time
    MDE['latitude'] = p1.latitude
    MDE['longitude'] = p1.longitude
    
    print(f'{Color.PURPLE} Saving data for {year}{Color.END}')
    enc_dict = {'zlib': True, 'complevel': 4, 'fletcher32': True, 'shuffle': True}
    vbs = [list(MDE.variables)[0]]
    enc = {i: enc_dict for i in vbs}
    saver.ch_paths(MDE, f'{mde_path}',f'e5.oper.an.pl.mde_{start_yr}_{vb}',vbs,counter=ct,sv_data_times='Numbered')
    #MDE.to_netcdf(f'{mde_path}/e5.oper.an.pl.mde_{year}.nc',encoding=enc)
    
    enc_dict = {'zlib': True, 'complevel': 4, 'fletcher32': True, 'shuffle': True}
    vbs = [list(MSE.variables)[0]]
    enc = {i: enc_dict for i in vbs}
    
    saver.ch_paths(MSE, f'{mse_path}',f'e5.oper.an.pl.mde_{start_yr}_{vb}',vbs,counter=ct,sv_data_times='Numbered')
    #MSE.to_netcdf(f'{mse_path}/e5.oper.an.pl.mde_{year}.nc',encoding=enc)
    #return mse
    print(f'{Color.GREEN} Done Saving data for {year}{Color.END}')

    ct=len(glob.glob(f'{mse_path}*.nc',recursive=True))

#mse_result = calculate_mse(temperature, specific_humidity, pressure)
#print(f'Moist Static Energy: {mse_result} J/kg')

def process_years(years):
    """
    Process multiple years in parallel.

    Parameters:
    - years: List of years to process
    """
    # Create a multiprocessing pool with the number of available CPU cores
    num_cores = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(processes=num_cores)
    
    # Map the calculate_me function to the list of years
    pool.map(calculate_me, years)
    
    # Close the pool
    pool.close()
    pool.join()

if __name__ == "__main__":
    # Define the range of years to process
    start_year = 1990
    end_year = 1995
    years = list(range(start_year, end_year + 1))
    
    # Process the years in parallel
    process_years(years)