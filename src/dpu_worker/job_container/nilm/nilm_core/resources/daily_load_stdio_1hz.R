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
app.usage <- app.usage.main(data.file, app.meta.json, 
	find.heavy=FALSE, 
	find.heavy.pattern=TRUE, 
	find.pattern=FALSE, 
	find.rice=TRUE,
	find.standby = TRUE, 
	find.cyclic=TRUE, 
	find.ac=TRUE, 
	find.washer=TRUE,
	show.fig=FALSE)
    
#
#
# ---------------------- End Main Code ----------------------------------
#
#


#print output
sink()
cat(toJSON(app.usage,pretty=FALSE))



