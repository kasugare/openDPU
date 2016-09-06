#!/usr/bin/Rscript

options(warn=-1)
sink('/dev/null')
Sys.setlocale("LC_ALL","en_US.UTF-8")


  #
  # Load libraries
  #
  library(NILM1Hz)

std.input <- readLines(file('stdin'))
input.split <- unlist(strsplit(std.input, split = "_encored_"))
data.file = input.split[[1]]
app.meta.json = input.split[[2]]

#
#
# ---------------------- Start Main Code ----------------------------------
#
#
app.usage <- app.usage.main(data.file, app.meta.json, find.heavy=T, find.heavy.pattern=T, find.pattern=F, find.rice=T, find.cyclic=T, find.ac=T, find.washer=T, find.standby=T, show.fig=F)
    
#
#
# ---------------------- End Main Code ----------------------------------
#
#


#print output
sink()
cat(toJSON(app.usage,pretty=FALSE))



