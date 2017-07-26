BEGIN{
    FS=",";
    OFS=",";

    frequency=(1.0*total_num/num_sample);
    split(frequency,a,".");
    frequency=a[1];

    candidate_count=1;
    sample_count=0;
}{
    if( NR == 1 && header==1 ) {
        if( $(NF) != "app" ) {
            print "something is wrong.........";
            exit;
        }else {
            print $0;
        }
    }
    if( sample_count >= num_sample ) exit;

    if( $(NF) == sample_val ){
        buf[candidate_count] = $0;
        candidate_count = candidate_count + 1;
        if( candidate_count >= frequency ){
            rand_num = int( frequency*rand() );        
            print buf[rand_num];
            sample_count = sample_count + 1;

            #reset counter
            candidate_count = 1;
        }
    } else {

    }
}END{

}
