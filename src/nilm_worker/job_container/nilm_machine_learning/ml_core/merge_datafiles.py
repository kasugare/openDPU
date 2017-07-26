#!/usr/bin/env python

#./merge_datafiles.py -d ./data -n tv_*_*_*.csv -s F3020026,F3FDFFCE -a tv -p 600 -o ./data/merged_08_tv.csv
#./merge_datafiles.py -d ./data -n tv_*_*_*.csv -a tv -p 600 -o ./data/merged_08_tv.csv -f ./data/merged_08_tv.feather
#
# ===== Convert from csv to feather =====
# import feather, pandas
# feather.write_dataframe(pandas.read_csv(file_csv, header=0), file_feather)
#
#
import os
import fnmatch
import optparse
import pandas as pd
import feather

def get_file_list(dir,name_expr,serials):
    target_files = []
    if serials is not None:
        serials_list = serials.split(',')

    for filename in os.listdir(dir):
        if len(filename.split('_')) < 2:
            continue

        if fnmatch.fnmatch(filename, name_expr) and (serials is None or filename.split('_')[1] in serials_list):
            target_files.append(filename)

    return target_files

parser = optparse.OptionParser()
parser.add_option("-d", "--dir", dest="dir",
                  help="Output directory")
parser.add_option("-n", "--name_expr", dest="name_expr",
                  help="Output filename prefix")
parser.add_option("-a", "--appliance", dest="appliance",
                  help="Target appliance name")
parser.add_option("-s", "--serials", dest="serials",
                  help="Serial numbers")
parser.add_option("-p", "--points", dest="points",
                  help="Number of points for sampling")
parser.add_option("-o", "--output", dest="output",
                  help="Output file")
parser.add_option("-f", "--feather", dest="feather",
                  help="Output in feather")
parser.add_option("-c", "--channel", dest="channel",
                  help="channel")

(options, args) = parser.parse_args()

merged = pd.DataFrame()
files = get_file_list(options.dir, options.name_expr, options.serials)
print '===== Reading file ====='
num_files = len(files)
num_processed = 1
for file in files:
    print num_processed, num_files, file
    num_processed += 1

    file_info = file.split('_')
    if len(file_info) > 1:
        serial = file_info[1]
    else:
        continue

    data = pd.read_csv(options.dir + '/' + file)

    appliance_columns = filter(lambda x: options.appliance in x,list(data.columns.values))
    if len(appliance_columns) < 1:
        print 'critical error. no appliance column'
        break

    data['sum'] = data.loc[:,appliance_columns].sum(axis=1,numeric_only=True)
    data[options.appliance] = map(lambda x: 1 if float(x)>0.0 else 0, data['sum'])

    data['siteid'] = serial
    column_list = ['siteid','timestamp']
    if options.channel is not None:
        for postfix in ['.x', '.y']:
            for p in ['ap','rp']:
                for i in range(1,int(options.points)+1):
                    column_list.append( p + '_' + str(i) + postfix)

    else:
        for p in ['ap', 'rp']:
            for i in range(1, int(options.points) + 1):
                column_list.append(p + '_' + str(i))
    column_list.append(options.appliance)
    data = data[column_list]

    merged = pd.concat([merged, data], axis=0)
    data = None

print '===== Start writing file ====='
merged.to_csv(options.output,index=False,header=True)
if options.feather is not None:
    feather.write_dataframe(merged,options.feather)
