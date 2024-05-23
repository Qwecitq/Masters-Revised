#!/usr/bin/env python3
# -*- coding: utf-8 -*-


#################################################################################
#            THIS SCTIPT IS USED TO CONCATENATE TEXT FILES TOGETHER AS ONE                  #
#################################################################################

import os
import random
exec(open('../imports.py').read())
import ast 

# Specify the folder containing the files
folder_path = "./"
algo='cascade_bard_v1'

# Change to the folder containing the files
os.chdir(folder_path)

file_path = 'random_locations.txt'

# Read the file and extract coordinates
with open(file_path, 'r') as file:
    lines = file.readlines()

coordinates = [ast.literal_eval(line.strip()) for line in lines]

# Convert coordinates to floats
float_coordinates = [(float(lat), float(lon)) for lon, lat in coordinates]

random_locations = float_coordinates
for rn_loc in random_locations:
    
    # Specify the output file
    output_file = f"ARS_lf_{rn_loc[1]}E_{rn_loc[0]}N_1980-2020_full.txt"

    # Get a list of all files in the folder
    files_to_concatenate = glob.glob(f'{folder_path}*{rn_loc[0]}E.{rn_loc[1]}N*.txt', recursive=True)
    files_to_concatenate.sort()
   
    print(files_to_concatenate)
    times = []
    # Iterate over each file and append its content to the output file
    for file_name in files_to_concatenate:
        
        with open(file_name, 'r') as input_file:
            lines = input_file.readlines()
            if len(lines) == 1: 
                
                to_write = [x[2:-5] for x in lines[0].split(',')]
                #print(to_write)
                times.append(to_write)
                # Open the output file in append mode
                
         
                        #output.close()
            else:   #if len(lines)>1:

                to_write = [x[:-2] for x in lines]
                times.append(to_write)
                #output.close()
                
    times = sum(times, [])
    
    #sv_folder = f'{algo}_randomlocs_times'
    
    #if os.path.isdir(sv_folder) == False:
    #    os.mkdir(sv_folder)
    # Open the output file in write mode
    with open(f'{folder_path}{output_file}', 'w') as output:
        for timesteps in times:
            #print(timesteps)
            output.write(f'{timesteps}\n')
                    
    # Print a message indicating the completion of the concatenation
    print(f"Concatenation completed. Output saved to {output_file}")

    rmfiles = [os.remove(x) for x in files_to_concatenate]