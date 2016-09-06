#!/usr/bin/Rscript

options(warn=-1)
sink('/dev/null')
Sys.setlocale("LC_ALL","en_US.UTF-8")

library(NILM1Hz)

data.file <- readLines(file('stdin'))

# --------------- Start Main Code -----------------------
meta.list.json <- app.detect.main(
  data.file = data.file,
	find.heavy=FALSE,
	find.patternHigh=TRUE,
	find.pattern=FALSE,
	find.pattern_extend=FALSE,
	find.rice=TRUE,
	find.standby=TRUE,
	find.cyclic=TRUE,
	find.ac=TRUE,
	find.washer=TRUE,
  check.data=TRUE,
	debug.mode=FALSE)
# ------------------------- End Main Code -----------------------------

sink()
cat(meta.list.json)