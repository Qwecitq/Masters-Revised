#!/bin/bash

# Define the path to your Python script
python_script="your_script.py"

vb='epv'
typ='epv'

# Loop over the years from 2000 to 2020
#for year in {2001..2010}; do
#    st_yr=$year
#    en_yr=$((year + 1))
    
    # Construct the savepath based on the year
savepath="./MERRA2-lagged"
algo=("cascade_bard_v1","SCAFET","guan_waliser","connect500","mundhenk_v3","reid250","gershunov","lora_v2") 
#algo='SCAFET'
#algo='guan_waliser'
#algo='connect500'
#algo='mundhenk_v3'
#algo='reid250'
#algo='gershunov'
#algo='lora_v2'
echo $vb $typ $savepath # $st_yr $en_yr 
# Run the Python script with input arguments
./merra_composites_ARs-lagged.py $vb $typ ${savepath}  $algo&
#done

#onyame tumfo wohw3 yenso daa