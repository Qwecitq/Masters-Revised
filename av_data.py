#!/usr/bin/env python3
# -*- coding: utf-8 -*-
exec(open('imports.py').read())
from tqdm import tqdm
import matplotlib.ticker as mtk
import dask 
import dask.delayed


# Define a function to normalize each variable in the dataset
@dask.delayed
def normalize_variable(var, var_type):
    ''' 
    This function normalises an xarray dataset for values between -1 and 1. 
    
    Arguments
    --------------
    
    var : xarray dataset or data array
    
    Return
    ----------
    
    xarray normalised dataset
    
    '''
    if var_type == 'levels':
        
        min_val = var.min(dim=['lon','lat'])    #obtain min value
        #print(min_val.values)
        max_val = var.max(dim=['lon','lat'])   # obtain max value
    elif var_type == 'surface':
        min_val = var.min()    #obtain min value  dim=['longitude','laitude']
        #print(min_val.values)
        max_val = var.max()   # obtain max value
        
    normalized_var = 2 * (var - min_val) / (max_val - min_val) - 1    #compute normalisation
    
    return normalized_var


algorithms = ['cascade_bard_v1', 'SCAFET', 'mundhenk_v3', 'lora_v2', 'reid250','connect500', 'gershunov', 'guan_waliser']
alg_names = ['TECA BARD v1','SCAFET', 'Mundhenk v3','Lora v2', 'Reid 250', 'Connect 500', 'Gershunov','Guan & Waliser']
variables = ['h','epv','omega','ivt','slp','tqv']
levs_2_plot = [925,850,700,500,300,250]

len_of_data = []
mpath='./MERRA2/'
for alx, alg in enumerate(algorithms):
    locals()[f'{alg}_merra'] = collections.defaultdict(list)
    print(f'{Color.PURPLE}{alg.upper()}{Color.END}')
    for vx,vb in enumerate(variables):
        pths = glob.glob(f'{mpath}conus_ARS_DJF_comps_{alg}_{vb}_raw/*.nc',recursive=True)
        pths.sort()
        #ds = [xr.open_dataset(pt, chunks='auto') for pt in pths]
        #ds = xr.concat(ds,dim='time')
        ds = xr.open_mfdataset(pths, chunks='auto')
        print('Done loading data \nCalculating mean')
        
        with open(f'cluster_numbers/cluster_values_{alg}') as clusters:
            cluster = clusters.readlines()
            cluster = [int(x[0]) for x in cluster]
        print(len(cluster))
            
        ds['time'] = cluster
        
        print('Loading for groups')
        groups = ds.groupby('time').groups.keys()
        for gr in groups:
            if vx <=2:
                typ = 'levels'
                dds = ds.sel(lev=levs_2_plot, time=gr).mean('time').load()
            elif vx>=3:
                typ = 'surface'
                dds = ds.sel(time=gr).mean('time').load()
            
            #eval(f'{alg}_merra')[f'{vb}_c{gr}'] = dds   #.load()  #xr.open_mfdataset(pths, chunks='auto')
            sv_path = f'Averages/'
            
           
            print('Calculating normalisations')
            dds[f'norm_{vb.upper()}'] = normalize_variable(dds,var_type=typ).compute()[vb.upper()]
            
            sv_path='Averages/'
            final_path = f'{sv_path}{alg}_cluster/'

            if os.path.isdir(sv_path) == False:
                os.mkdir(sv_path)

            if os.path.isdir(final_path) == False:
                os.mkdir(final_path)
            
            dds.to_netcdf(f'{final_path}{alg}_cluster{gr}_{vb}.nc')
            
            
        #print(len(pths))
        
        #if vx == 0 :
            #len_of_data.append(len(ds.time.values))