BEGIN{
    FS = ",";
    OFS = ",";
    on_count = 0;
    off_count = 0;
}{
    if( $(NF) == 1 ) {
        on_count = on_count + 1;
    } else if( $(NF) == 0 ){
        off_count = off_count + 1;
    }
}END{
    print on_count+off_count,on_count,off_count;
}
