#!/bin/bash

#ardt=( 'cascade_bard_v1' 'mundhenk' 'guan_waliser' 'reid250' ) #name of ardt 

#ardt=( "guan_waliser" )
#regs=( 'south_africa' 'south_america' 'north_america' )
#blat=( -48 -60 25 ) #bottom latitude
#llon=( -25 -115 -160 )  #left longitude 

regs=( 'conus' )
blat=( 25 ) #bottom latitude
llon=( -140 )  #left longitude 

threshold=0.8 # sys.argv[4] #set the threshold for the selection of landfall (usually between 0 and 1)
algo='SCAFET'

#This takes a cordinate and expands to the right and top of it (i.e, if (60,15), then lon 60 to 61 and lat 15 to 16 will be selected as the box
#echo 'What is the landfall region? (input : lon/lat)'
#read lfreg
#['south_africa', 'south_america','north_america']
#lfreg=( "16.5/-28" "-73.5/-44" "-122/37" )  
#lfreg=("81/25" )
#echo "What months do you like to select? (Dec,Jan,Feb, or Feb,Mar,Apr or Apr,Jul,Oct...)"

#read seas
seas='Dec,Jan,Feb'
sv_path='./NoCal'
#for lf in "${!lfreg[@]}"; do
./ARS_lf_ts_saver.py ${regs} ${blat} ${llon} $threshold $seas $sv_path $algo&
    
#done