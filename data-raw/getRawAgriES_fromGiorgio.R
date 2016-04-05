## Function to read table Eurostat from drive of Giorgio (received March 2016)

pathway = "~/Desktop/FAO/Trade/Raw_data/"

getRawAgriES_fromGiorgio <- function(year, path) {
  esdata_raw <- tbl_df(data.table::fread(paste0(path,"/eu_cn8_dump_",year,".csv"),
                                         header = T, sep = ";",
                                         colClasses = c("integer","integer","character",
                                                        "integer","integer","character",
                                                        "double","double","double"))) %>%
    select(-stat_regime) %>%
    mutate(hs6 = stringr::str_sub(product_nc,1,6),
           period = as.numeric(stringr::str_sub(period,1,4)))
  colnames(esdata_raw) = c("reporter","partner","hs","flow","year","value","weight","qty","hs6")
  name = paste0("esdata_",year)
  save(esdata_raw, file = paste0("~/Desktop/FAO/Trade/RData/",name,".Rdata"))

  esdata_raw
}

for(i in 2009:2014){
  getRawAgriES_fromGiorgio(i, pathway)
}
