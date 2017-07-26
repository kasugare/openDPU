#!/usr/bin/Rscript
library(getopt);
library(magrittr)

optSpec <- matrix(c(
  'siteid','s',1,"character", "site id (required)",
  'app','a',1,"character", "appliance code (required)",
  'tempdir','d',1,"character", "temporaly folder",
  'version','v',1,"character", "git code version",
  'trainData','r',1,"character", "training data",
  'testData','e',1,"character", "test data",
  'prefix', 'p',1,"character", "prefix",
  'dirname', 'f',1,"character", "result folder",
  'help'   , 'h', 0, "logical",   "this help"
),ncol=5,byrow=T)

opt = getopt(optSpec);
#opt$siteid  <- '0001,0002'
opt$summary <- dir(opt$tempdir,full.names=T) #'/tmp/RtmpV1yUxA/summary462c54f364c8.rds,/tmp/RtmpV1yUxA/summary462c54f364c8.rds'
#opt$app <- '68'
#opt$comments <- '사용 빈도가 낮을 경우 정확도가 낮을 수 있다. '
#opt$version <- 'seonjeonglee@ForceEvent@11111 '

TARGET_APPLIANCES <- switch (opt$app,
                             '12' = 'Tv',
                             '62' = 'CyclicLoad',
                             '63' = 'CyclicLoad',
                             '64' = 'CyclicLoad',
                             '66' = 'RiceCooker',
                             '67' = 'Washer',
                             '68' = 'HeavyLoad')
if(is.null(TARGET_APPLIANCES)){
  cat('Invalid appliance code\n')
  quit("yes")
}

siteIds <- strsplit(opt$siteid, split = ",")[[1]]
summaryResults <- opt$summary #strsplit(opt$summary, split = ",") #[[1]]
version <- opt$version
trainData <- opt$trainData
testData <- opt$testData
comment <- opt$comments
comment <- c( comment,
              paste("Training Data :", lapply( stringr::str_split(trainData, "--")[[1]],
                                               function(x) format(as.POSIXct(x), '%F %H:%M:%S')) %>%
                      unlist() %>%
                      paste(collapse=' -- ')))
comment <- c( comment,
              paste("Test Data :", lapply( stringr::str_split(testData, "--")[[1]],
                                           function(x) format(as.POSIXct(x), '%F %H:%M:%S')) %>%
                      unlist() %>%
                      paste(collapse=' -- ')))
prefix <- opt$prefix
dirname <- opt$dirname

cat(siteIds)
destroyForce::generateReport(
  appliance = TARGET_APPLIANCES,
  siteIds,
  summaryResults,
  insertSubFigures = FALSE,
  version,
  comment,
  prefix,
  dirname
)
