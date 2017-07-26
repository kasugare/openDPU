#!/bin/bash

#
# ex) ./make_sample.sh -f "/disk1/raw_data_with_plug/dl-by-device/merged_2ch/*.csv" -n 100
#

#
# option parser
#
while [[ $# -gt 1 ]]
do
key="$1"

case $key in
    -f|--files)
    files="$2"
    shift # past argument
    ;;
    -n|--num_sample)
    num_sample="$2"
    shift # past argument
    ;;
   --default)
    DEFAULT=YES
    ;;
    *)
            # unknown option
    ;;
esac
shift # past argument or value
done

#
# @filename
# @sample_val
#

#files="/disk1/raw_data_with_plug/dl-by-device/merged_2ch/*.csv"

# get stat 
for filename in $files
do
    echo "Processing $filename file..."

    onoff_output=`cat $filename | awk -f get_simple_stat.awk` 
    IFS=', ' read -r -a onoff_stat <<< "$onoff_output"

    num_on=${onoff_stat[1]}
    num_off=${onoff_stat[2]}

    echo "Number of On: $num_on, Number of Off: $num_off"
    if [ "$num_on" -lt "$num_sample" -o "$num_off" -lt "$num_sample" ]
    then
        echo "Sample is not enough. Skip this file..."
        continue

    else
        sample_val=0
        tot_num_sample=$num_off
        cat $filename | awk -f get_sample.awk -v sample_val=$sample_val -v total_num=$tot_num_sample -v num_sample=$num_sample -v header=1 > $filename.$num_sample.sample.csv

        sample_val=1
        tot_num_sample=$num_on
        cat $filename | awk -f get_sample.awk -v sample_val=$sample_val -v total_num=$tot_num_sample -v num_sample=$num_sample -v header=0 >> $filename.$num_sample.sample.csv
    fi
done

