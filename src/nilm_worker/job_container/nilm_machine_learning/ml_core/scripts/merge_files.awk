#!/usr/bin/awk
# awk -v serial="Fxxxxxxx" -v nch=1
BEGIN{
    FS = ",";
    OFS = ",";
    header_printed = 0;
    idx_begin_app = 2 + 1200*nch + 1;

    if( length(serial) != 8 ) {
        exit;
    }
}
{
    if( $1==$1+0 ){
        sub_ap_sum = 0.0;
        for( i=3; i<603; i++ ) {
            sub_ap_sum = sub_ap_sum + $i;
        }
        if( sub_ap_sum > 0.0 ){
            app_sum = 0.0;
            for( a=idx_begin_app; a<=NF; a++ ){
                app_sum = app_sum + $a
            }
            printf "%s",serial OFS;
            for( c=2; c<idx_begin_app; c++) {
                printf "%s",$c OFS;
            }
            printf "%s\n",app_sum;
        }
    }else {
        if( header_printed==0 ) {
            printf "%s","siteid" OFS;
            for( c=2; c<idx_begin_app; c++) {
                printf "%s",$c OFS;
            }
            printf "app\n";
            header_printed = 1;
        }
    }
}

#
# #!/bin/bash
# FILES=/disk1/raw_data_with_plug/dl-by-device-good/processed/*
# for f in $FILES
# do
#  filename=$(basename "$f")
#  IFS='-' read -r -a filename_split <<< "$filename"
#  echo "Processing $f file... $filename ${filename_split[1]}"
#  # take action on each file. $f store current file name
#  cat $f/*.csv | awk -f ../merge_files.awk -v serial="${filename_split[1]}" -v nch=1 > $filename.csv
# done
