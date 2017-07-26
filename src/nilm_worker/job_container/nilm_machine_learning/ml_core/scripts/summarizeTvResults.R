#!/usr/bin/Rscript
library(methods)
library(getopt);
library(magrittr)
#devtools::install_github("seonjeonglee/destroyForce@19962ad")
library(destroyForce)

optSpec <- matrix(c(
        'result', 'r',1,"character", "file for result (required)",
        'method','m',1,"character", "on/off threshold (required)",
        'country','c',1,"character", "country (required)",
        'tempdir','d',1,"character", "temporaly folder",
        'help'   , 'h', 0, "logical",   "this help"
),ncol=5,byrow=T)
opt = getopt(optSpec);


TARGET_APPLIANCES <- 'Tv'
inDataPathForResult <- opt$result
method <- opt$method
country <- opt$country
timezone <- opt$timezone
dirPathForSummmaryResults <- opt$tempdir #ifelse(is.null(opt$dirPath), tempdir(), opt$dirPath)

summaryResult <- destroyTV_interface(
  inDataPathForResult ,
  method = method
)

tempFileName <- tempfile(pattern = 'summary', tmpdir = dirPathForSummmaryResults, fileext = '.rds')
if(!dir.exists(dirname(tempFileName))){
  dir.create(dirname(tempFileName))
}
saveRDS(object = summaryResult, file = tempFileName)
cat(tempFileName,'\n')
cat("Done\n")
