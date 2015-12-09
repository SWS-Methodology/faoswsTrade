library(dplyr)

faosws::GetTestEnvironment(
  baseUrl = "https://hqlqasws1.hq.un.fao.org:8181/sws", # QA?
  token = "da889579-5684-4593-aa36-2d86af5d7138") # http://hqlqasws1.hq.un.fao.org:8080/sws/


swsdomain <- "trade"
swsdataset <- "completed_tf_fcl"
swsdimension <- "measuredItemFS"


fclcodelist <- faosws::GetCodeList(swsdomain, swsdataset, swsdimension)

save(fclcodelist, file = "data/fclcodelist.RData")
