library(NILM1Hz)
library(plyr)
library(reshape)
library(lubridate)
library(evaluateNILM)
library(ggplot2)
library(RJSONIO)

#x=read.csv('./data/nilm_input_test.csv')
#names(x)=c('timestamp','voltage','current','active_power','reactive_power','apparent_power','power_factor')
x$timestamp = as.numeric(x$timestamp)
x$timestamp = as.POSIXct(x$timestamp,tz="Asia/Seoul",origin="1970-01-01")

y = x[!duplicated(x),]

#basic data check
if( difftime( y$timestamp[ max(which(y$active_power > 10)) ],
 y$timestamp[ min(which(y$active_power > 10)) ], units='hours') < 4 )
stop('Not enough data : cyclic')

if( length(which(y$active_power < quantile( y$active_power, .05) + 10 )) / length(y$active_power) > .5 )
 stop('Less than the base amount : cyclic')

if( length(which(y$active_power < 0 )) / length(y$active_power) > .25 )
 stop('Negative active power : cyclic')

if( length(which(y$active_power < 10 )) / length(y$active_power) > .8 )
 stop('No appliance : cyclic')


#find cyclic boxes
meta.list.cyclic = list()
meta.list.cyclic <- tryCatch(
{
	generate.meta.info(y,max.iter=3,debug.mode=FALSE)
}, error = function(err) {
	list()
})

#find heavy loads
meta.list.high = list()
meta.list.high <- tryCatch(
{
	high.power.detect(y,max.iter=3,debug.mode=FALSE)
}, error = function(err) {
        list()
})

#merge list
meta.list = list()
if( length(meta.list.cyclic) > 0 ) meta.list = append(meta.list,meta.list.cyclic)
if( length(meta.list.high) > 0 ) meta.list = append(meta.list,meta.list.high)

if( length(meta.list) > 0 ) {
	names(meta.list)=seq(0,length(meta.list)-1)
	meta.list.json <- toJSON( meta.list,pretty=TRUE )
}else{
	meta.list.json <- ''
}

#print meta.list
#cat(meta.list.json)

