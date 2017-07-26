import os.path
#
# Get sequential filename 
#
def next_filename(filename, ext=None):
    if ext is None:
        postfix = ''
    else:
        postfix = ext

    if not os.path.exists(filename+postfix):
        return filename + postfix

    num = 1
    while True:
        next_filename = (filename+'_%d' + postfix) % num
        if not os.path.exists(next_filename):
            return next_filename
        num += 1

#
# Transform into a list from directory or comma seperated file names 
#
def file_path_to_list(file_path):
    if os.path.isdir(file_path):
        input_files = [ f for f in os.listdir(file_path) if os.path.isfile(os.path.join(file_path,f))]
        input_files = ','.join(map(lambda x: os.path.join(file_path,x),input_files))
    else:
        input_files = file_path

    return input_files.split(',')


#
# Create default logger
#
def create_logger(logger_name):
    import logging 

    # create logger
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    logger.debug('start')

    return logger


#
# make a merged form data from plug raw data...
#
def merge_rows_from_encored_plug(serial,filename):
    import pandas as pd

    temp_buffer_ap1 = []
    tansformed_row = []

    with open(filename) as fp:
        count_total = 0
        for oneline in fp:
            arr = oneline.split(',')
            count_sec = count_total % 60
            if count_sec == 0:
                if temp_buffer_ap1 is not None and len(temp_buffer_ap1) == 600 :
                    tansformed_row.append( [serial, float(timestamp)] + temp_buffer_ap1 + temp_buffer_rp1 + [1] )

                timestamp = arr[0]
                temp_buffer_ap1 = []
                temp_buffer_rp1 = []
            temp_buffer_ap1 = temp_buffer_ap1 + map(lambda x: float(x), arr[3:13])
            temp_buffer_rp1 = temp_buffer_rp1 + map(lambda x: float(x), arr[13:23])
                
            count_total = count_total + 1

        transformed_pd = pd.DataFrame(tansformed_row)
        transformed_pd.columns = ['siteid','timestamp'] + [str(i) for i in range(0,1200)] + ['onoff']

        return transformed_pd


