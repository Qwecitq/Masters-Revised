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


algorithms = ['gershunov']
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
            
            
            


scaler = StandardScaler()
#####################################################################################
# Note to self: To do the pv and sh stacking for kmeans computation, I have to make the data into 2D. So #
# I first concat along the longitude or latitude (based on which transects), then collapse the lon and lat di-  #
# mensions. This gives an array index of ntime, 2*lon*lat. I do this in the next 2 lines of code                      #
####################################################################################
data1= eval(f'{alg}_merra')['epv']['EPV'].sel(lev=500)
data2 =eval(f'{alg}_merra')['ivt']['IVT']
new_data = xr.concat([data1,data2],dim=['lat','lon']) #reshape using this! More memory efficient
#new_data = np.reshape(trans_data.values,(len(trans_data.time.values),len(trans_data.level)*len(trans_data[tloc].values)))
ddss = new_data
new_data = new_data.stack(lev_lat=('lon','lat','concat_dim')).transpose('time', 'lev_lat') #reshape using this! More memory efficient
#new_data = np.reshape(new_data.values,(len(trans_data.time.values),len(trans_data.level)*len(trans_data[tloc].values)))

print(len(new_data.time.values))
print('Scaling data')
print('Using numpy to scale')
num_of_splits = 55
dividend = np.floor_divide(len(new_data.time.values),num_of_splits)

#set the dividend for dividing the dataset into pieces for making the scaling easier on memory 
#nd1 = new_data.isel(time=slice(0,dividend))                #set dividend 
 
for ix,nx in tqdm(enumerate(np.arange(0,len(new_data.time.values),dividend)),desc='Scaling'):
    print(f'Scaling for data {ix} with dividend {nx}')
    st = nx
    ed = nx+dividend
    
    if ix <num_of_splits-1:
        res =  new_data.isel(time=slice(nx,nx+dividend))
    elif ix ==num_of_splits-1:
         res = new_data.isel(time=slice(nx,None))
        
    new_res = np.nan_to_num(np.array((res-res.mean('time'))/res.std('time')),nan=0)
    #new_res = np.nan_to_num(scaler.fit_transform(res) , 0)
    
    locals()[f'nd{ix}'] = new_res
    
scaled_data = np.concatenate([eval(f'nd{x}') for x in range(num_of_splits)])
print(scaled_data.shape)
#scaled_data = np.nan_to_num(np.array((new_data-new_data.mean('time'))/new_data.std('time')),nan=0) #not memory efficient but faster and also replaces nans with 0

#print('Scaling data with Dask')
# Use Dask to handle larger-than-memory data
#new_data_dask = da.from_array(new_data, chunks='auto')
#scaled_data = scaler.fit_transform(new_data_dask)
# Convert Dask array to NumPy array
#scaled_data = np.nan_to_num(scaled_data, nan= 0)




from sklearn.cluster import MiniBatchKMeans

K=np.arange(2,10)
silhouette_scores=[]

#scaled_data = np.memmap('SCAFET_scaled.txt', mode='r')
#np.loadtxt('SCAFET_scaled.txt')

# Define a function containing the for loop and decorate it with @njit
#@njit#(nopython=False)
def compute_silhouette_scores(K, scaled_data):
    
    silhouette_scores = []
    for k in K:
        
        print(f'Starting Kmeans for k={k}')
        kmeans = MiniBatchKMeans(n_clusters=k, random_state=0)
        clustered_data = kmeans.fit_predict(scaled_data)
        print('Done fitting predicted \nApplying Silhoette averages')
        silhouette_avg = silhouette_score(scaled_data, clustered_data)
        silhouette_scores.append(silhouette_avg)
    return silhouette_scores

# Define the range of K values
K = np.arange(2, 10)

# Call the function to compute silhouette scores
silhouette_scores = compute_silhouette_scores(K, scaled_data)

# Find the index with the maximum silhouette score
k_index = np.argmax(silhouette_scores)

print(f'{Color.BLUE} The number of preferred clusters for {algorithms[0]} is {K[k_index]}{Color.END}')

# Plot the Elbow Method curve
plt.plot(K, silhouette_scores, 'bx-')
plt.xlabel('Number of Clusters (k)')
plt.ylabel('Silhouette Scores')
plt.title('Silhouette Method for Optimal k')
plt.text(x=5,y=0.065,s=f'Highest Silhouette Score : {round(silhouette_scores[k_index],3)}')
plt.axhline(y= silhouette_scores[k_index],linestyle='dashed', color='grey')

plt.savefig(f'Images/Silhouette_scores_{alg}.png')

k = K[k_index]
kmeans = KMeans(n_clusters=k, random_state=0)

print('Fitting scaled data')
clustered_data = kmeans.fit_predict(scaled_data)

cluster_path = 'cluster_numbers/'

if os.path.isdir(cluster_path)==False:
    os.mkdir(cluster_path)

with open(f'{cluster_path}cluster_values_{alg}', 'w') as output:
        for lines in clustered_data:
            #print(timesteps)
            output.write(f'{lines}\n')


print(f'{Color.RED}Done {Color.END}')