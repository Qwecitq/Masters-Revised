#!/bin/bash


# Specify the folder path
folder_path="climatological_composites/"

# Check if the folder exists
if [ ! -d "$folder_path" ]; then
    # If the folder doesn't exist, create it
    mkdir -p "$folder_path"
    echo "Folder created: $folder_path"
else
    echo "Folder already exists: $folder_path"
fi

vb='t'
typ='levels'

# Loop over the years from 2000 to 2020
#for year in {2001..2010}; do
#    st_yr=$year
#    en_yr=$((year + 1))
    
    # Construct the savepath based on the year
savepath="./climatological_composites"

#algo='cascade_bard_v1'
#algo='SCAFET'
algo='guan_waliser'
#algo='connect500'
#algo='mundhenk_v3'
#algo='reid250'
#algo='gershunov'
echo $vb $typ $savepath # $st_yr $en_yr 
# Run the Python script with input arguments
./clim_composites_LPS_MCS.py $vb $typ ${savepath}  $algo&
#done

#onyame tumfo wohw3 yenso daa