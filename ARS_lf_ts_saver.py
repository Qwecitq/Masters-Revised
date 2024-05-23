#!/usr/bin/env python3
# -*- coding: utf-8 -*-


'''THIS SCRIPT SELECTS THE LANDFALLING REGIONS FOR RANDOM LOCATIONS IN A SPECIFIED LONLAT BOX AND SEARCHES IN TIME IF THERE IS A LANDFALL OF MCS? '''

exec(open('imports.py').read())
import dask
import random
from tqdm import tqdm 
from concurrent.futures import ProcessPoolExecutor
import multiprocessing
import ast 


#### CHANGE ME ######
reg=sys.argv[1]

#for reg in regs: 
#input details from user
#ardt = str(sys.argv[1])                                               #name of ardt 

blat = sys.argv[2] ; blat = int(blat)                             #bottom latitude
llon = sys.argv[3] ; llon = int(llon)                              #left longitude 

threshold = sys.argv[4] ; threshold = float(threshold)                             #set the threshold for the selection of landfall (usually between 0 and 1)

seas = sys.argv[5]                                         #this selects the seasons or the period the user wants to select for landfalling ARs
seas = seas.split(',')


#select the landfall region
#lfreg = sys.argv[6]
#lfreg = lfreg.split('/')                                       #split the string input by ','
#lfreg = [float(x) for x in lfreg]                        #convert the string numbers into floats

sv_path=sys.argv[6]                                    #location to save data
algo=sys.argv[7]

if os.path.isdir(sv_path)==False:
    os.mkdir(sv_path)

final_path = f'{sv_path}/{reg}_ARS_{algo}/'

if os.path.isdir(final_path)==False:
    os.mkdir(final_path)
    
# Check for command-line arguments
if len(sys.argv) < 6:
    print(f'{Color.RED}Error: Insufficient command-line arguments.{Color.END}')
    print(f'{Color.YELLOW}Usage: python script.py region blat llon threshold seasons lfreg sv_path{Color.END}')
    sys.exit(1)

print(f'{Color.GREEN} Running for Superimposed datasets for MCS {Color.END}')

#############################################################################################################
#############################################  RECEIVED INPUTS  ##############################################
#############################################################################################################
#check if all inputs are right 

print(f'Variable name: ARs \nBottom Latitude: {blat} \nLeft Longitude: {llon} \nThreshold: {threshold} \nSeasons: {seas} ')



#############################################################################################################
########################################## Tunable Parameters #################################################
#############################################################################################################

#path to the dataset for selection
mpath = '/global/cfs/projectdirs/m1517/cascade/external_datasets/ARTMIP/tier1/ftp_mirror/' 

#You can change this to suit what you want 

lat_width =    40                                 #region latitude width
lon_width = 40                               #region longitude width 
'''
#landfall region box width in both lon and lat directions
#In this case, we are looking at the indian region so we select MCS that fall within a latitude accross the entire indian region's longitude, that is from 68 to 94 longitudes and within the latitudinal band of 1
mid_point = (94+68)/2 
#set the latitude and longitude range
tlat = blat + lat_width
rlon = llon + lon_width'''

#############################################################################################################
############################################ Select Season ####################################################
#############################################################################################################


selection_box_width = 1      
selection_box_height = 1
# Specify the latitude and longitude box
min_latitude = blat
max_latitude = blat + lat_width
min_longitude = llon
max_longitude = llon + lon_width 

file_path = 'NoCalconus_boundary.txt'

# Read the file and extract coordinates
with open(file_path, 'r') as file:
    lines = file.readlines()

coordinates = [ast.literal_eval(line.strip()) for line in lines]

# Convert coordinates to floats
float_coordinates = [(float(lat), float(lon)) for lon, lat in coordinates][::6]

# Print the randomly selected locations
for i, location in enumerate(float_coordinates, 1):
    print(f"{Color.YELLOW}Location {i}: Latitude {location[0]}, Longitude {location[1]}{Color.END}")

    #save the random locations as a text file
with open(f'{final_path}random_locations.txt','w') as f :
            for line in float_coordinates:
                f.write(f'{line}\n')
                
#assign the calendar months into numbers
months_in_year = [calendar.month_abbr[m] for m in np.arange(1,13)] 
months_idx = {mon:idx+1 for idx,mon in enumerate(months_in_year)}

#select the period the user wants to select landfall for
period = [months_idx.get(se) for se in seas]


#############################################################################################################
############################################## Load data ######################################################
#############################################################################################################

#mcs data path 
ars_path =f'/global/cfs/projectdirs/m1517/cascade/external_datasets/ARTMIP_NCAR_mirror/catalogues/tier1/{algo}/'

varbs = ['ar_binary_tag']
dpts = []

ct=0

def process_year(yx):
    
    ars_ps = glob.glob(f'{ars_path}/*{yx}*.nc4', recursive=True)
    print('Done tracing paths')

    #datasets = [dask.delayed(xr.open_mfdataset)(mcs_ps)]
    
    #dataset = datasets[0].compute()
    #data = dataset.chunk({'time':'auto'}).sel(time=dataset.time.dt.month.isin(period))
    datasets = [xr.open_dataset(ix, chunks='auto') for ix in tqdm(ars_ps, desc='Loading Progress')]
    dataset = xr.concat(datasets,dim='time')
    data = dataset.sel(time=dataset.time.dt.month.isin(period))
    print(f'Loading datasets for {yx} ...')
    print(f'Done loading and chunking data for {yx}')

    #############################################################################################################
    ######################################## Set Threshold for selection  ##############################################
    #############################################################################################################
    
    #times=[]
    
        
    #select the region for the landfall test 
    for rn_loc in float_coordinates:
        
        #check if the data lons and lats are not transformed 
        if data.lon.max()>=359:
            
            ix = rn_loc[1] 
        elif data.lon.max() >= 179:
            ix = (rn_loc[1] + 180) %360 - 180
            
        iy = rn_loc[0]
        print(data)
        selection_box = data['ar_binary_tag'].sel(lon=slice(ix, ix+selection_box_width),
                                lat=slice(iy,  iy +selection_box_height))

        print(selection_box)
        #check if the threshold criteria is met within the selected region 

        sel_box=selection_box.chunk({'time':1}).compute()
        selected_times= sel_box.time.where(sel_box.mean(['lon','lat']) >= threshold,drop=True)['time'].values
        print(f'{Color.PURPLE}Done selecting timesteps {Color.END}')

        #print(f'Done with selection for {yx} \n{len(selected_times)}')
        #selected_data = data.sel(time=selected_times)

        #sv_times = list(selected_data.time.values)
        #############################################################################################################
        ########################################### Begin Saving TImesteps  #############################################
        #############################################################################################################
        selected_times = [str(ix) for ix in tqdm(selected_times,desc='Converting Times')]
        #times.append(selected_times)
        selected_times = [selected_times]
        print(f'{Color.BLUE}Done with  year {yx} for location {rn_loc}...{Color.END}')

        #np.savetxt(f'{final_path}MCS_lf_ts_reg_0E.10N_1x1grids.txt',times, delimiter=',')

        with open(f'{final_path}ARs_lf_ts_reg_{algo}_{rn_loc[1]}E.{rn_loc[0]}N_1x1grids-{yx}.txt','w') as f :
            for line in selected_times:
                f.write(f'{line}\n')
                
def run_process_year_and_store_results(year):
    process_year(year)

def main():
    # Set the range of years you want to process
    start_year = 1980
    end_year = 2018

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
    
    
    
    
    