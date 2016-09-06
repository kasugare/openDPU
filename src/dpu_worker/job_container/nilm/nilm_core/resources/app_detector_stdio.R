#!/usr/bin/Rscript

options(warn=-1)
sink('/dev/null')
Sys.setlocale("LC_ALL","en_US.UTF-8")


usePackage <- function(p) {
  if (!is.element(p, installed.packages()[,1]))
    install.packages(p, dep = TRUE)
  require(p, character.only = TRUE)
}

r <- getOption("repos")
r["CRAN"] <- "http://cran.us.r-project.org"
options(repos = r)
rm(r)
lapply(c("RJSONIO",'ggplot2','plyr','lubridate','scales','reshape'),usePackage)

  # Library
  #
  library(NILM1Hz)
  library(plyr)
  library(reshape)
  library(lubridate)
#  library(evaluateNILM)
  library(ggplot2)
  library(RJSONIO)

data.file <- readLines(file('stdin'))

#
#
# --------------- Start Main Code -----------------------
#
#

meta.list.json <- app.detect.main(
      data.file = data.file, find.heavy=F, find.patternHigh=T, find.pattern=F, find.pattern_extend=F,
      find.rice=T, find.standby=T, find.cyclic=T, find.ac=T,find.washer=T,
      check.data=T, debug.mode=F)
# 
#
# ------------------------- End Main Code -----------------------------
#
#
sink()
cat(meta.list.json)

