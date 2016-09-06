#!/usr/bin/Rscript

options(warn=-1)
sink('/dev/null')
Sys.setlocale("LC_ALL","en_US.UTF-8")


library(nilm15hz)

data.file <- readLines(file('stdin'))

# --------------- Start Main Code -----------------------
meta.list.json <- app.detect.main(
      data.file = data.file, find.heavy=F, find.patternHigh=T, find.pattern=F, find.pattern_extend=F,
      find.rice=T, find.standby=T, find.cyclic=T, find.ac=T,find.washer=T,find.tv=T,
      check.data=T, debug.mode=F)
# ------------------------- End Main Code -----------------------------

sink()
cat(meta.list.json)
# rm(list=ls())
# gc(reset=TRUE)
