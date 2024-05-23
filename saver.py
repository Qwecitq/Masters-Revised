exec(open('imports.py').read())
from metpy.interpolate import cross_section
from tqdm import tqdm

def split_by_chunks(dataset):
    chunk_slices = {}
    for dim, chunks in dataset.chunks.items(): #get the chunks from the data loaded by dask 
        slices = [] #a list of the slices 
        start = 0 #start to count the chucks 
        for chunk in chunks: #specific chunks 
            if start >= dataset.sizes[dim]:  #if the number of start >= size of the dataset which should not be, break
                break
            stop = start + chunk #set the end point for the chunk 
            slices.append(slice(start, stop)) #create a slice object from the chuck start to stop and append to slices 
            start = stop #reset the start point to the end point of the previous chunk's end point
        chunk_slices[dim] = slices #set the dictionary chunk_slices to a specific time chunk 
    for slices in itertools.product(*chunk_slices.values()): #use the slices created together with the itertools.product to call the chunck slices 
        selection = dict(zip(chunk_slices.keys(), slices)) #create a dict and a zip of the slices 
        yield dataset[selection]#apply the selection zipped dict on the data to select the chunks 


def create_filepath(ds, prefix='filename', root_path=".",sv_data_times=None):
    """
    Generate a filepath when given an xarray dataset
    """
    
    #do this if you want to use the stock file naming which adds the first 10 letters of the time variable in the data 
    #else, add your own texts that differentiate the saved data for different chunks as sv_data_times
    if len(sv_data_times)>0:
        
        filepath = f'{root_path}{prefix}.nc'
        
    else:
        
        start = str(ds.time.data[0])[:18]
        end = str(ds.time.data[-1])[:18]
        print(f'{start} and {end}')
        filepath = f'{root_path}{prefix}_{start}_{end}.nc'
        
    return filepath

def ds_len(ds):
    
    '''This collects the length of the time dimension for the data'''
    dlen = len(ds.time)
    return dlen

def ch_paths(data,loc,pfix,vb,counter, sv_data_times=False):
    '''
    
    This automates the whole saving process and saves the files for the dataset
    
    Arguments
    --------------
    
    data : data to be saved in netcdf format (`xr.Dataset()`)
    loc :   location to store dataset
    pfix :  prefix for storing the data
    vb :   variable name for data encoding (this should be the same as the variable name found in the dataset)
    sv_data_times : default (`None`) change this to a list of the prefixes that you want to use.
    
    Return ch_paths
    
    '''
    
    in_data = list(split_by_chunks(data))

    dlens = [ds_len(ds) for ds in tqdm(in_data, desc='Chunk data time lengths')]
    dlens = np.cumsum(dlens)
    dlens = sum([[0],list(dlens)], []) #combine the cum sum with a leading 0
    
    #print(dlens)
    #print(in_data[0])
    paths=[]
    
    #if isinstance(sv_data_times, str):
    if type(sv_data_times) == str:
        print('True')
        paths = [f'{loc}{pfix}_{str(sv_num)}.nc' for sv_num in tqdm(np.arange(counter , counter+len(in_data)),desc='Cr8ing Paths')]
        print(paths)
        #start_time = 
        #end_time = 
        #paths = [f'{loc}{pfix}_{ind.time.values[0][:10]}_{ind.time.values[-1][:10]}' for ind in in_data]
        
    elif sv_data_times is None: 
        paths = [create_filepath(ds,prefix=pfix, root_path=loc, sv_data_times = None) for ds in in_data]
        print(paths)
        
    #elif isinstance(sv_data_times, list):
    elif type(sv_data_times)==list:
        print('True')
        #print(sv_data_times[dlens[1]])
        #sets the paths from the sv_data_times using the corresponding index generated from the len of the in_data.time
        paths = [create_filepath(ds,pfix+f'{sv_data_times[d]}_{sv_data_times[d-1]}', loc, sv_data_times=sv_data_times) for ds,d in zip(in_data, dlens)]
        print(paths)
        #dlens = [len(ds.times) for ds in in_data]
        
    
    
        
    enc_dict =  {'zlib':True,'complevel': 1, 'fletcher32': True}#:[1,1,len(in_data[0].latitude.values),len(in_data[0].longitude.values)]}
    
    enc = { i: enc_dict for i in vb }
    t=xr.save_mfdataset(in_data,paths,encoding=enc)
    return t

