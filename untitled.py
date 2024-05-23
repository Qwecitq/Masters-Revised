#!/usr/bin/env python3
# -*- coding: utf-8 -*-


exec(open('imports.py').read())
from tqdm import tqdm 
import matplotlib.ticker as mtk
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
import concurrent.futures
from functools import partial
import dask.array as da
from numba import njit

algorithms = ['cascade_bard_v1', 'SCAFET', 'mundhenk_v3', 'lora_v2', 'reid250','connect500', 'gershunov', 'guan_waliser']
total_data = []


print(f'{Color.GREEN} Computing Kmeans cluster for {algorithms[0]}{Color.END}')
#,'SCAFET','mundhenk_v3','reid250','connect500','gershunov','guan_waliser']
variables = ['epv','ivt']
len_of_data = []
mpath='./MERRA2/'

for alx, alg in enumerate(algorithms):
    locals()[f'{alg}_merra'] = collections.defaultdict(list)
    print(f'{Color.PURPLE}{alg.upper()}{Color.END}')
    for vx,vb in enumerate(variables):
        pths = glob.glob(f'{mpath}conus_ARS_DJF_comps_{alg}_{vb}_raw/*.nc',recursive=True)
        print(len(pths))
        pths.sort()
        #ds = [xr.open_dataset(pt, chunks='auto') for pt in pths]
        
        #ds = xr.concat(ds,dim='tixme')
        ds = xr.open_mfdataset(pths, chunks='auto')
        print('Done loading data \nCalculating mean')
        #dds = ds.mean('time')
        eval(f'{alg}_merra')[vb] = ds#.load()  #xr.open_mfdataset(pths, chunks='auto')
        print(len(pths))
        
        if vx == 0 :
            len_of_data.append(len(ds.time.values))

