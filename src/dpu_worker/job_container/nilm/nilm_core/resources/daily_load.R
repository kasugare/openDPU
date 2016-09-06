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

y=x[!duplicated(x),]

app.meta = fromJSON(app.meta.json)
app.usage = 0.0
shape_type = app.meta[['shape_type']] 
if( shape_type == 'cyclic_box' ) {
	temp <- find.box.shape(app.meta, y)
	temp <- post.processing( data.frame( timestamp = y$timestamp, p = temp$ap.box, q = temp$rp.box ) )
	app.usage = sum(temp$p)/3600.
}else if( shape_type == 'high_power' ) {
        temp <- meta.to.box.Shape2(data = y, json.para = app.meta ) 
	app.usage = sum(temp$ap.box)/3600.
}

#meta.list = list()
#meta.list[[1]] <- fromJSON(app.meta.json)
#cat(toJSON(meta.list,pretty=TRUE))
#box.out <- meta.to.box.Shape( y[1:(3600*2),], meta.list, Debug.mode=FALSE)
#app.usage = sum(box.out[[1]]$p)/3600.



