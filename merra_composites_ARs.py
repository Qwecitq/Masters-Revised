#!/usr/bin/env python3
# -*- coding: utf-8 -*-


'''THIS SCRIPT SELECTS THE CONSENSUS LPS AND MCS COMPOSITES FROM THE ERA5 REPO'''

exec(open('imports.py').read())
import dask
from tqdm import tqdm 
from concurrent.futures import ProcessPoolExecutor
import multiprocessing
import ast

#variables in this dataset are EPV, SLP, QV,
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
if vb == 'slp':
    mpath=f'/global/cfs/cdirs/m3522/cmip6/MERRA2/'
    loc = 'inst1_2d_asm_Nx/'
    #'/global/cfs/cdirs/m3522/cmip6/MERRA2/
    dpths = glob.glob(f'{mpath}{loc}*/*.nc',recursive=True)
    dpths.sort()
    lon_name = 'lon'
    lat_name = 'lat'
    
elif vb == 'epv' or vb == 'qv' or vb == 'h' or vb == 'omega':
    mpath=f'/global/cfs/cdirs/m3522/cmip6/MERRA2/'
    loc = 'inst3_3d_asm_Np/'
    dpths = glob.glob(f'{mpath}{loc}*/*.nc4',recursive=True)
    dpths.sort()
    lon_name = 'lon'
    lat_name = 'lat'
    
elif vb == 'tqv':
    mpath =f'/global/cfs/cdirs/m3522/cmip6/MERRA2/'
    loc = 'ARTMIP_MERRA2_native_1hour/'
    dpths = glob.glob(f'{mpath}{loc}*.tavg1_2d_slv_Nx.*nc',recursive=True)
    dpths.sort()
    lon_name = 'longitude'
    lat_name = 'latitude'

#this is specially for potential temperature since we calculated the data ourselves
elif typ =='pot':
    mpath = f'../ARs Work/Non-coinciding ARDTs/Bash Runs/'
    loc = f'PT_{reg}/'
    dpths = glob.glob(f'{mpath}{loc}*/*_{vb}.*.nc4',recursive=True)
    dpths.sort()
    lon_name = 'longitude'
    lat_name = 'latitude'
    
elif typ == 'ivt':
    mpath =f'/global/cfs/cdirs/m3522/cmip6/MERRA2/'
    loc = 'ARTMIP_MERRA2_native_1hour/'
    dpths = glob.glob(f'{mpath}{loc}*_hourly{vb.upper()}*.nc',recursive=True)
    dpths.sort()
    lon_name = 'lon'
    lat_name = 'lat'

elif typ=='precip':
    mpath=f'/global/cfs/projectdirs/m4374/catalogues/raw_catalogue_files/observations/PFs_category/20*/MERGED_FP/GPM*.nc'
    dpths = glob.glob(f'{mpath}', recursive=True)
    dpths.sort()
    #print(dpths)
    lon_name = 'longitude'
    lat_name = 'latitude'
    
print(mpath)


#print(dpths)
if os.path.isdir(sv_path)==False:
    os.mkdir(sv_path)

final_path = f'{sv_path}/{reg}_ARS_DJF_comps_{algo}_{vb}_raw/'
if os.path.isdir(final_path)==False:
    os.mkdir(final_path)

    
def process_year(r):
    
    #try:
    #for r in tqdm(range(int(start_yr),int(end_yr)),desc='Years Completed'):
    formal_path=f'conus_ARS_{algo}/'
    file_path = f'{formal_path}random_locations.txt'

    # Read the file and extract coordinates
    with open(file_path, 'r') as file:
        lines = file.readlines()

    coordinates = [ast.literal_eval(line.strip()) for line in lines]

    # Convert coordinates to floats
    float_coordinates = [(float(lat), float(lon)) for lon, lat in coordinates]
    if vb != 'ivt':
        dp = [a for a in dpths if f'.{str(r)}' in a]            #data paths specific to the selected year
    elif vb == 'ivt':
        dp = [a for a in dpths if f'{str(r)}' in a]            #data paths specific to the selected year
    if vb =='precipitationCal':
        dp =  [a for a in dpths if f'/{str(r)}/' in a]  
    #print(dp)
        
    ds = [xr.open_dataset(a, chunks='auto') for a in tqdm(dp, desc=f'Loading Data for {r}')]
    print('Concatenating Data ...')

    ds = xr.concat(ds, dim='time').drop_duplicates(dim='time')
    print(ds)
    #clim_pths = glob.glob(f'../SWA_work/climatologies_1980-2020/clim_{vb}*',recursive=True)        #load climatologies 
    if vb == 'ivt' or vb == 'tqv' or vb == 'precipitationCal':
        ds[vb.upper()] = ds[vb]
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


        times.sort()
        #select times from dataset 
        new_ds = ds.sortby('time').sel(time=times,method='nearest')

        #convert data lons from 0-360 to -180-180
        new_ds.coords[lon_name] = (new_ds.coords[lon_name]  + 180) % 360 - 180 #convert from 0-360 to -180 to 180
        new_ds = new_ds.sortby(new_ds[lon_name]) #sort the lons

        #Convert lons (ix) into -180 to 180
        ix = (ix + 180)% 360 - 180
        print(new_ds.longitude.values)
        ######################################################################################
        ########################## CREATE SUPERIMPOSED POSITION DATASET ###################
        ######################################################################################

        sup_imposed_ds = []
        #set the distance from the center of the landfall point
        ln_dist = 25   #in degrees
        lt_dist = 15   #in degrees

        if vb == 'precipitationCal':
            _nds = new_ds.sel(longitude=slice( ix - ln_dist,  ix + ln_dist), 
                         latitude = slice(iy - lt_dist, iy + lt_dist))
            
        # if vb != 'tqv' or vb !='precipitationCal':
        #     _nds = new_ds.sel(lon=slice(ix - ln_dist, ix + ln_dist), 
        #                  lat = slice(iy - lt_dist, iy + lt_dist))
        elif vb == 'tqv':
            _nds = new_ds.sel(longitude=slice(ix - ln_dist, ix + ln_dist), 
                         latitude = slice(iy - lt_dist, iy + lt_dist))
        

        print(_nds)
        #for ln,lt,tms in zip(lons,lats):
            #for a specific time, select the data such that, ln,lt is the center of the data 
         #   _nds = ds.sel(time=times).sel(lon=slice(ln-ln_dist, ln+ln_dist), lat=slice(lt+lt_dist, lt-lt_dist))
            #_clm = clim_ds.sel(lon=slice(ln-ln_dist, ln+ln_dist), lat=slice(lt+lt_dist, lt-lt_dist))

            #_nds = _nds - _clm
        lon_data_resolution = abs(_nds[lon_name][0].values - _nds[lon_name][1].values) ; print(lon_data_resolution)#0.625   #for MERRA2
        lat_data_resolution =  abs(_nds[lat_name][0].values - _nds[lat_name][1].values) ; print(lat_data_resolution)
        #set new lon and lat values to the dataset
        lon_range = np.arange(-ln_dist,ln_dist+0.1, lon_data_resolution)
        lat_range = np.arange(-lt_dist, lt_dist+0.1, lat_data_resolution)

        print(lon_range)
        print(lat_range)

        time_range = np.arange(counter,counter+len(times),1)

        counter = counter + len(times)

        #reassign new lons and lats to the dataset 
        _nds[lat_name] = lat_range[:len(_nds[lat_name].values)] ; _nds[lon_name] = lon_range[:len(_nds[lon_name].values)]   ; _nds['time'] = time_range

        #append dataset to list 
        data_to_save.append(_nds.sortby(lon_name))
        #ds = ds.sel(lon=slice(-20,20),lat=slice(20,0))
    print(f'{Color.RED}{data_to_save[0]}{Color.END}')
    new_dataset = xr.Dataset()

    new_dataset[vb.upper()] = xr.concat([x for x in data_to_save if len(x.time)> 0 ],dim='time' )
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

    #except:
    #   pass
def run_process_year_and_store_results(year):
    process_year(year)

def main():
    # Set the range of years you want to process
    start_year = 2001
    end_year =  2017

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
    
    