## Function to read table tariffline UNSD from drive of Giorgio (received March 2016)

pathway = "~/Desktop/FAO/Trade/Raw_data/"

getRawAgriTL_fromGiorgio <- function(year, path) {
  tldata_raw <- tbl_df(data.table::fread(paste0(path,"/tariffLine_dump_",year,".csv"),
                                         header = T, sep = ";",
                                         colClasses = c("integer","integer","character",
                                                        "integer","character","integer",
                                                        "integer","double","integer","double"))) %>%
    select(-hsrep)
  colnames(tldata_raw) = c("reporter","year","flow","hs","partner","qunit","weight","qty","value")
  name = paste0("tldata_",year)
  save(tldata_raw, file = paste0("~/Desktop/FAO/Trade/RData/",name,".Rdata"))
}

for(i in 2009:2014){
  getRawAgriTL_fromGiorgio(i, pathway)
}
