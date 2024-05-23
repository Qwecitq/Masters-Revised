#!/usr/bin/env python3
# -*- coding: utf-8 -*-


'''THIS SCRIPT SELECTS THE CONSENSUS LPS AND MCS COMPOSITES FROM THE ERA5 REPO'''

exec(open('imports.py').read())
import dask
from tqdm import tqdm 

#########################################################################################
#                                                                                 LOAD DATASETS                                                               # 
#########################################################################################
reg = 'sw_africa'
lag=[12,18]

vb = sys.argv[1]
typ=sys.argv[2]
start_yr = sys.argv[3]
end_yr = sys.argv[4]
sv_path=sys.argv[5]

times_to_sel= open('sw_africa_MCS_1x1-savedtimes/intersection_MCS_LPS_lonlat_0E10N_1x1grid.txt','r')   #open text data generated from LPS_MCS_lf_ts_saver.sh
times_to_sel = times_to_sel.readlines()    #read data into a variable 
times = [t.split('\n')[0] for t in times_to_sel]

#Set ERA5 data repo path
if typ == 'surface':
    mpath=f'/global/cfs/cdirs/m3522/cmip6/ERA5/'
    loc = 'e5.oper.an.sfc/'
    
elif typ == 'levels':
    mpath=f'/global/cfs/cdirs/m3522/cmip6/ERA5/'
    loc = 'e5.oper.an.pl/'
    
#this is specially for potential temperature since we calculated the data ourselves
elif typ =='pot':
    mpath = f'../ARs Work/Non-coinciding ARDTs/Bash Runs/'
    loc = f'PT_{reg}/'

print(mpath)
dpths = glob.glob(f'{mpath}{loc}{start_yr}*/*_{vb}.*.nc',recursive=True)
dpths.sort()



#iterate over the lagged times you want

for lg in tqdm(lag, desc='Processing lags', unit='lag'):
    
    #create paths for saving composites 
    if os.path.isdir(sv_path)==False:
        os.mkdir(sv_path)

    final_path = f'{sv_path}/{reg}_MCS_LPS_1x1-JJAS_comps_{vb}_lag{lg}hrs/'
    if os.path.isdir(final_path)==False:
        os.mkdir(final_path)
        
    #loop through data range and select specific year duration and save 
    if start_yr=='2000':
        ct=0
    else:
        ct = len(glob.glob(f'{final_path}*.nc',recursive=True))
        
    print(f'Computing for lag {lg}')
    for r in tqdm(range(int(start_yr),int(end_yr)),desc='Years Completed'):

        dp = [a for a in dpths if str(r) in a]            #data paths specific to the selected year
        ts = [t for t in times if str(r) in t ]               # data times specific to the selected year for the LPS_MCS co-occurrence

        
        #ts = ts[~ts.duplicated()]

        print(f'Loading data for {r}')

        ds = [xr.open_dataset(a, chunks='auto') for a in tqdm(dp, desc=f'Loading Data for {r}')]
        print('Concatenating Data ...')

        ds = xr.concat(ds, dim='time').drop_duplicates(dim='time')
        # Convert ts to a pandas DatetimeIndex
        #ts_index = pd.to_datetime(ts)

        # Use isin to filter the dataset
        #ds = ds.sel(time=ds['time'].astype(str).isin(ts))
        # Convert the list of times to pandas Timestamp objects
        times_to_select = pd.to_datetime(ts)

        # Find the indices of the times in the dataset
        indices = np.searchsorted(ds['time'].values, times_to_select)

        # Adjust indices to ensure they are within the valid range
        indices = np.clip(indices - lg, 0, len(ds['time']) - lg)

        print(ts)
        
        ds = ds.isel(time=indices)
        print(ds)

        ds.coords['longitude'] = (ds.coords['longitude']  + 180) % 360 - 180 #convert from 0-360 to -180 to 180
        ds = ds.sortby(ds.longitude) #sort the lons
        ds = ds.sel(longitude=slice(-20,20),latitude=slice(20,0))

        ds = ds.chunk({'time':6})

        sv_times = ds.time.values
        #print(sv_times)
        print(f'Started saving for {r} ...')

        vbs = list(ds.variables)#
       # [3:-3]
        print(vbs)
        print(ds)
        saver.ch_paths(ds, f'{final_path}',f'e5.oper.an_{start_yr}_{vb}',vbs,counter=ct,sv_data_times='Numbered')    

        
        print(f'Done with {r} ...')
        ct=len(glob.glob(f'{final_path}*.nc',recursive=True)) 
    print(f'Done with all years for lag {lg}')
