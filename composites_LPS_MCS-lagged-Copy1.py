#!/usr/bin/env python3
# -*- coding: utf-8 -*-


'''THIS SCRIPT SELECTS THE CONSENSUS LPS AND MCS COMPOSITES FROM THE ERA5 REPO'''

exec(open('imports.py').read())
import dask
from tqdm import tqdm 
from concurrent.futures import ProcessPoolExecutor
import multiprocessing


#########################################################################################
#                                                                                 LOAD DATASETS                                                               # 
#########################################################################################
reg = 'sw_africa'
lag=[4]#,6]
#lag=[8,12]

vb = sys.argv[1]
typ=sys.argv[2]
#start_yr = sys.argv[3]
#end_yr = sys.argv[4]
sv_path=sys.argv[3]

# ANSI escape codes for colors
class Color:
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    PURPLE = '\033[95m'
    CYAN = '\033[96m'
    END = '\033[0m'

# Check for command-line arguments
if len(sys.argv) < 2:
    print(f'{Color.RED}Error: Insufficient command-line arguments.{Color.END}')
    print(f'{Color.YELLOW}Usage: python script.py region blat llon threshold seasons lfreg sv_path{Color.END}')
    sys.exit(1)
    
times_to_sel= open('sw_africa_MCS_1x1-savedtimes/intersection_MCS_LPS_lonlat_0E10N_1x1grid.txt','r')   #open text data generated from LPS_MCS_lf_ts_saver.sh
times_to_sel = times_to_sel.readlines()    #read data into a variable 
ti_lo_la = [t.split('\n')[0] for t in times_to_sel]


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
dpths = glob.glob(f'{mpath}{loc}*/*_{vb}.*.nc',recursive=True)
dpths.sort()



#iterate over the lagged times you want
print(f'{Color.GREEN} Lagged Supperimposed Composites {Color.END}')

def process_year(r):
    
    #for r in tqdm(range(int(start_yr),int(end_yr)),desc='Years Completed'):

    dp = [a for a in dpths if str(r) in a]            #data paths specific to the selected year
    ts = [t for t in ti_lo_la if str(r) in t ]               # data times specific to the selected year for the LPS_MCS co-occurrence
    print(ts)
    times,lons,lats = [], [], []

    #obtain the times and coordinates for the dataset
    for d in ts:
        dd = d.split(',')
        times.append(dd[0])
        lons.append(float(dd[1]))
        lats.append(float(dd[2]))
        
    ds = [xr.open_dataset(a, chunks='auto') for a in tqdm(dp, desc=f'Loading Data for {r}')]
    print(f'{Color.PURPLE}Concatenating Data ...{Color.END}')

    ds = xr.concat(ds, dim='time').drop_duplicates(dim='time')
    clim_pths = glob.glob(f'climatologies_1980-2020/clim_{vb}*',recursive=True)        #load climatologies 
    clim_ds = xr.open_mfdataset(clim_pths).mean(['bnds','time'])
    
    #calculate anomalies
    ds = ds[vb.upper()]-clim_ds[vb]   
    # Convert the list of times to pandas Timestamp objects
    times_to_select = pd.to_datetime(times)

    # Find the indices of the times in the dataset
    indices = np.searchsorted(ds['time'].values, times_to_select)


    for lg in tqdm(lag, desc='Processing lags', unit='lag'):
        
        print(f'{Color.GREEN}Computing for lag {lg}{Color.END}')
        #create paths for saving composites 
        if os.path.isdir(sv_path)==False:
            os.mkdir(sv_path)

        final_path = f'{sv_path}/{reg}_MCS_LPS_1x1-JJAS_comps_{vb}_lag{lg}hrs/'
        if os.path.isdir(final_path)==False:
            os.mkdir(final_path)

        #loop through data range and select specific year duration and save 
        if r=='2000':
            ct=0
        else:
            ct = len(glob.glob(f'{final_path}*.nc',recursive=True))
            
        # Adjust indices to ensure they are within the valid range
        tim_indices = np.clip(indices - lg, 0, len(ds['time']) - lg)

        #print(times)
        
        #ds = ds.isel(time=indices)
        

        ds.coords['longitude'] = (ds.coords['longitude']  + 180) % 360 - 180 #convert from 0-360 to -180 to 180
        ds = ds.sortby(ds.longitude) #sort the lons
        
        ######################################################################################
        ########################## CREATE SUPERIMPOSED POSITION DATASET ###################
        ######################################################################################

        sup_imposed_ds = []
        #set the distance from the center of the landfall point
        ln_dist = 20   #in degrees
        lt_dist = 10   #in degrees

        for ln,lt,tms in zip(lons,lats,tim_indices):
            #for a specific time, select the data such that, ln,lt is the center of the data 
            _nds = ds.isel(time=tms).sel(longitude=slice(ln-ln_dist, ln+ln_dist), latitude=slice(lt+lt_dist, lt-lt_dist))
            #print(_nds)
            #_clm = clim_ds.sel(longitude=slice(ln-ln_dist, ln+ln_dist), latitude=slice(lt+lt_dist, lt-lt_dist))

            #_nds = _nds - _clm
            data_resolution = 0.25   #for ERA5 
            #set new longitude and latitude values to the dataset
            lon_range = np.arange(-ln_dist,ln_dist+data_resolution, data_resolution)
            lat_range = np.arange(lt_dist+data_resolution, -lt_dist, -data_resolution)

            #reassign new lons and lats to the dataset 
            _nds['latitude'] = lat_range ; _nds['longitude'] = lon_range   

            #append dataset to list 
            sup_imposed_ds.append(_nds.sortby('longitude'))
        #ds = ds.sel(longitude=slice(-20,20),latitude=slice(20,0))

        new_dataset = xr.Dataset()

        new_dataset[vb.upper()] = xr.concat([x for x in sup_imposed_ds],dim='time' )
        #ds = ds.sel(longitude=slice(-20,20),latitude=slice(20,0))

        ds = ds.chunk({'time':6})

        enc_dict = {'zlib': True, 'complevel': 1, 'fletcher32': True}
        vbs = [list(new_dataset.variables)[0]]
        enc = {i: enc_dict for i in vbs}

        #new_dataset = new_dataset.load()
        new_dataset.to_netcdf(f'{final_path}MCS_LPS_supperimposed_{vb}_{r}.nc',encoding=enc)
        print(f'{Color.BLUE} Done with {r} for lag {lg} ...{Color.END}')
print(f'{Color.GREEN}Done with all years {Color.END}')

    
def run_process_year_and_store_results(year):
    process_year(year)

def main():
    # Set the range of years you want to process
    start_year = 2000
    end_year = 2010

    # Create a pool of processes
    pool = multiprocessing.Pool()

    # Use the pool to map the function over the range of years
    pool.map(run_process_year_and_store_results, range(start_year, end_year))

    # Close the pool
    pool.close()
    pool.join()

if __name__ == "__main__":
    main()
    
print(f'{Color.GREEN}Done with all years {Color.END}')