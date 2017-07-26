#!/usr/bin/env python

# Examples)
# rm -rf temp/report_temp; ./gen_report.py -d ./temp -s ./scripts -t F3020026,F302002E,F3020053 -r "2016-08-01-000000--2016-10-31-000000" -e "2016-09-21-000000--2016-09-30-000000" -c "Parameters_format=encompact01_epoch=30_points=600_model=cnn_1d_num_sample=5000" -a 12
# rm -rf temp_ac/report_temp; ./gen_report.py -d ./temp_ac -s ./scripts -t F3020026,F302002E,F3020053 -r "2016-08-01-000000--2016-10-31-000000" -e "2016-09-21-000000--2016-09-30-000000" -c "Parameters_format=encompact01_epoch=5_points=3000_model=cnn_1d_num_sample=1000" -a 65
#
import optparse
import subprocess
import os
import glob
parser = optparse.OptionParser()
parser.add_option("-d", "--dir", dest="dir",
                  help="Input directory")
parser.add_option("-t", "--report_site", dest="report_site",
                  help="Sites to report")
parser.add_option("-s", "--scripts", dest="scripts",
                  help="Path to R script ")
parser.add_option("-c", "--comment", dest="comment",
                  help="comments")
parser.add_option("-r", "--train", dest="train",
                  help="Date to train")
parser.add_option("-e", "--test", dest="test",
                  help="Date to test")
parser.add_option("-a", "--appliance", dest="appliance",
                  help="Appliance (12=Tv, 65=Ac)")
(options, args) = parser.parse_args()

if options.report_site is not None:
    report_site = options.report_site.split(',')
else:
    report_site = None

files= glob.glob(options.dir + '/*.csv')

processes = set()
max_processes = 1
    
rscriptOneSite = options.scripts + '/summarizeTvResults.R'
method = "2" # on if >= 0.5, 1: on if any value
tempOutput = options.dir + '/report_temp'
try:
    os.stat(tempOutput)
except:
    os.mkdir(tempOutput)  

serial_list = []
for filename in files:
    serial = filename.split('/')[-1].split('_')[0]
    if report_site is not None and serial not in report_site:
        continue
    serial_list.append(serial)
    #serial_list.append(onlyname.split('_')[0])
    command = rscriptOneSite 
    opts = " -r " + filename + \
    " -m " + method + \
    " -c " + 'JP' + \
    " -d " + tempOutput                                                                                                                                                            
    print command + opts                  
    os.system( command + opts )
    #processes.add(subprocess.Popen([command + opts], shell=True))


rscriptReport = options.scripts + '/makeReports.R' #-modified.R'
gitCodeHash = 'NA'
comments = options.comment #'format=encompact01;epoch=30;points:600;model=cnn_1d;num_sample=3000'
trainDateStr = options.train #'2016-08-01-000000--2016-10-31-000000'
testDateStr = options.test #'2016-08-01-000000--2016-10-31-000000'

testAvailableSerials = ','.join(map(lambda x: str(int(x,16)), serial_list))
command = rscriptReport + " -s " + testAvailableSerials + \
" -d " + tempOutput + \
" -v " + gitCodeHash + \
" -a " + options.appliance + \
" -r " + trainDateStr + \
" -e " + testDateStr  
#" -c " + comments + \
print '=========== make report ============='                                                                                                                                             
print(command)
os.system(command)     


