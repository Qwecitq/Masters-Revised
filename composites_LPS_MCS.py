#!/usr/bin/env python3
# -*- coding: utf-8 -*-


'''THIS SCRIPT SELECTS THE CONSENSUS LPS AND MCS COMPOSITES FROM THE ERA5 REPO'''

exec(open('imports.py').read())
import dask
from tqdm import tqdm 
from concurrent.futures import ProcessPoolExecutor
import multiprocessing
import ast

#########################################################################################
#                                                                                 LOAD DATASETS                                                               # 
#########################################################################################
reg = 'conus'

vb = sys.argv[1]
typ=sys.argv[2]
#start_yr = sys.argv[3]
#end_yr = sys.argv[4]
sv_path=sys.argv[3]
algo=sys.argv[4]

# Check for command-line arguments
if len(sys.argv) < 2:
    print(f'{Color.RED}Error: Insufficient command-line arguments.{Color.END}')
    print(f'{Color.YELLOW}Usage: python script.py region blat llon threshold seasons lfreg sv_path{Color.END}')
    sys.exit(1)

print(f'{Color.GREEN} Running for Superimposed datasets for {algo}...{Color.END}')
#Load the MCS_LPS co-occuring events dataset 

#print(ti_lo_la)

#Set ERA5 data repo path
if typ == 'surface':
    mpath=f'/global/cfs/cdirs/m3522/cmip6/ERA5/'
    loc = 'e5.oper.an.sfc/'
    dpths = glob.glob(f'{mpath}{loc}*/*_{vb}.*.nc',recursive=True)
    dpths.sort()
    
elif typ == 'levels':
    mpath=f'/global/cfs/cdirs/m3522/cmip6/ERA5/'
    loc = 'e5.oper.an.pl/'
    dpths = glob.glob(f'{mpath}{loc}*/*_{vb}.*.nc',recursive=True)
    dpths.sort()

#this is specially for potential temperature since we calculated the data ourselves
elif typ =='pot':
    mpath = f'../ARs Work/Non-coinciding ARDTs/Bash Runs/'
    loc = f'PT_{reg}/'
    dpths = glob.glob(f'{mpath}{loc}*/*_{vb}.*.nc',recursive=True)
    dpths.sort()
    
elif typ == 'ivt':
    mpath = f'../era5_IVT/'
    loc = ''
    dpths = glob.glob(f'{mpath}*_{vb}*.nc',recursive=True)
    dpths.sort()
    
print(mpath)


#print(dpths)
if os.path.isdir(sv_path)==False:
    os.mkdir(sv_path)

final_path = f'{sv_path}/{reg}_ARS_DJF_comps_{algo}_{vb}_raw/'
if os.path.isdir(final_path)==False:
    os.mkdir(final_path)

    
def process_year(r):
    
    #for r in tqdm(range(int(start_yr),int(end_yr)),desc='Years Completed'):
    formal_path=f'conus_ARS_{algo}/'
    file_path = f'{formal_path}random_locations.txt'

    # Read the file and extract coordinates
    with open(file_path, 'r') as file:
        lines = file.readlines()

    coordinates = [ast.literal_eval(line.strip()) for line in lines]

    # Convert coordinates to floats
    float_coordinates = [(float(lat), float(lon)) for lon, lat in coordinates]
    dp = [a for a in dpths if str(r) in a]            #data paths specific to the selected year
    ds = [xr.open_dataset(a, chunks='auto') for a in tqdm(dp, desc=f'Loading Data for {r}')]
    print('Concatenating Data ...')

    ds = xr.concat(ds, dim='time').drop_duplicates(dim='time')
    #clim_pths = glob.glob(f'../SWA_work/climatologies_1980-2020/clim_{vb}*',recursive=True)        #load climatologies 
    
    if typ == 'levels' or typ== 'pot':
        
        ds = ds[vb.upper()].sel(level=[850,700,500,300,250])  #I have selected only the 500 and 250hpa 
    else: 
        
        ds = ds[vb.upper()] 
    counter = 0
    
    data_to_save = []
    for rn_loc in float_coordinates:
        
        #remember ix is lon and iy is lat 
        ix = rn_loc[0]
        iy = rn_loc[1]
        
        #remember that in naming the text timesteps, there was a swap mistake in lon and lat in the name
        times_to_sel= open(f'{formal_path}ARS_lf_{iy}E_{ix}N_1980-2020_full.txt','r')   #open text data generated from LPS_MCS_lf_ts_saver.sh
        times_to_sel = times_to_sel.readlines()    #read data into a variable 
        ti_lo_la = [t.split('\n')[0] for t in times_to_sel]


        
        ts = [t for t in ti_lo_la if str(r) in t ]               # data times specific to the selected year for the LPS_MCS co-occurrence
        print(ts)
        times,lons,lats = [], [], []
        #no_time = []

        #obtain the times and coordinates for the dataset
        for d in ts:
            dd = d.split(',')
            print(dd)
            if len(dd[0]) > 3:
                times.append(dd[0])
            
           
        #ts = ts[~ts.duplicated()]
        print(f'{times}')
        #print(f'Loading data for {r}')

        
        
        #select times from dataset 
        new_ds = ds.sel(time=times,method='nearest')

        #convert data lons from 0-360 to -180-180
        new_ds.coords['longitude'] = (new_ds.coords['longitude']  + 180) % 360 - 180 #convert from 0-360 to -180 to 180
        new_ds = new_ds.sortby(new_ds.longitude) #sort the lons
        
        #Convert lons (ix) into -180 to 180
        ix = (ix + 180)% 360 - 180
            
        ######################################################################################
        ########################## CREATE SUPERIMPOSED POSITION DATASET ###################
        ######################################################################################

        sup_imposed_ds = []
        #set the distance from the center of the landfall point
        ln_dist = 25   #in degrees
        lt_dist = 15   #in degrees
        
        _nds = new_ds.sel(longitude=slice(ix - ln_dist, ix + ln_dist), 
                     latitude = slice(iy + lt_dist, iy - lt_dist))
        #for ln,lt,tms in zip(lons,lats):
            #for a specific time, select the data such that, ln,lt is the center of the data 
         #   _nds = ds.sel(time=times).sel(longitude=slice(ln-ln_dist, ln+ln_dist), latitude=slice(lt+lt_dist, lt-lt_dist))
            #_clm = clim_ds.sel(longitude=slice(ln-ln_dist, ln+ln_dist), latitude=slice(lt+lt_dist, lt-lt_dist))

            #_nds = _nds - _clm
        data_resolution = 0.25   #for ERA5 
        #set new longitude and latitude values to the dataset
        lon_range = np.arange(-ln_dist,ln_dist+data_resolution, data_resolution)
        lat_range = np.arange(lt_dist+data_resolution, -lt_dist, -data_resolution)
        
        
        time_range = np.arange(counter,counter+len(times),1)
        
        counter = counter + len(times)
        
        #reassign new lons and lats to the dataset 
        _nds['latitude'] = lat_range ; _nds['longitude'] = lon_range   ; _nds['time'] = time_range

        #append dataset to list 
        data_to_save.append(_nds.sortby('longitude'))
        #ds = ds.sel(longitude=slice(-20,20),latitude=slice(20,0))
    
    new_dataset = xr.Dataset()

    new_dataset[vb.upper()] = xr.concat([x for x in data_to_save],dim='time' )
    time_range = pd.date_range(str(r),periods=len(new_dataset.time.values), freq='1H')
    new_dataset['time'] = time_range 
    new_dataset.chunk({'time':6})
    
    enc_dict = {'zlib': True, 'complevel': 1, 'fletcher32': True}
    vbs = [list(new_dataset.variables)[0]]
    enc = {i: enc_dict for i in vbs}

    new_dataset = new_dataset.load()
    
    if algo == 'g' or algo=='S':
        # Get the total number of time steps
        total_time_steps = len(new_dataset.time)

        # Define the indices for the three subsets
        subset1_indices = slice(0, total_time_steps // 3)
        subset2_indices = slice(total_time_steps // 3, 2 * (total_time_steps // 3))
        subset3_indices = slice(2 * (total_time_steps // 3), total_time_steps)

        # Create three subsets
        subset1 = ds.isel(time=subset1_indices)
        subset2 = ds.isel(time=subset2_indices)
        subset3 = ds.isel(time=subset3_indices)
        
        sub_list = [subset1,subset2,subset3]
        
        sub_len = np.arange(0,len(sub_list))
        
        saving = [x.to_netcdf(f'{final_path}ARS_supperimposed_{vb}_{r}_{algo}_{ix}.nc',encoding=enc) for ix,x in enumerate(sub_list)]
        #subset1.to_netcdf(f'{final_path}ARS_supperimposed_{vb}_{r}_{algo}.nc',encoding=enc)
        #subset2.to_netcdf(f'{final_path}ARS_supperimposed_{vb}_{r}_{algo}.nc',encoding=enc)
        #subset3.to_netcdf(f'{final_path}ARS_supperimposed_{vb}_{r}_{algo}.nc',encoding=enc)
    else:
        new_dataset.to_netcdf(f'{final_path}ARS_supperimposed_{vb}_{r}_{algo}.nc',encoding=enc)
    #saver.ch_paths(new_dataset, f'{final_path}',f'ARS_supperimposed_',vbs,counter=counter,sv_data_times='False')
    print(f'{Color.BLUE} Done with {r} for {algo}...{Color.END}')

def run_process_year_and_store_results(year):
    process_year(year)

def main():
    # Set the range of years you want to process
    start_year = 1980
    end_year = 2000

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


'''
    #concatenate dataset 
    
    ds = new_dataset.chunk({'time':10})
    
    sv_times = ds.time.values
    #print(sv_times)
    print(f'Started saving for {r} ...')
        
    vbs = list(ds.variables)#
   # [3:-3]
    print(vbs)
    print(ds)
    saver.ch_paths(ds, f'{final_path}',f'e5.oper.an_{start_yr}_{vb}',vbs,counter=ct,sv_data_times='Numbered')    
    
    ct=len(glob.glob(f'{final_path}*.nc',recursive=True)) '''
    
    