#!/bin/bash

vb='v'
typ='levels'

# Loop over the years from 2000 to 2020
#for year in {2001..2010}; do
#    st_yr=$year
#    en_yr=$((year + 1))
    
    # Construct the savepath based on the year
savepath="./Lagged_data"

echo $vb $typ $savepath # $st_yr $en_yr 
# Run the Python script with input arguments
./composites_LPS_MCS-lagged-Copy1.py $vb $typ ${savepath} &
#done

#onyame tumfo wohw3 yenso daa