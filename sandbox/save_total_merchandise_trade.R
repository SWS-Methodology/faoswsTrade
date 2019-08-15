library(faosws)
library(dplyr)
library(data.table)

# This scripts read an Excel file with total merchandise trade data
# and uploads its contents to the total trade dataset.
# NOTE: the original file has both FAO and WTO data, the latter for
# 2014 and onwards. WTO data was then uploaded as total_merchandise_trade
# datatable, so it gets removed from here.


# File to load
f <- "C:/Users/mongeau.FAODOMAIN/Dropbox/GitHub/SWS-Methodology/faoswsTrade/sandbox/Total_merchandise_trade_1961_2017.xlsx"

SetClientFiles("C:/Users/mongeau.FAODOMAIN/Documents/certificates/qa")

# QA
GetTestEnvironment(baseUrl = "https://hqlqasws1.hq.un.fao.org:8181/sws", token = "74fd7083-e5f8-468e-880e-ad4d8f04a819")



load_data <- function(xlsfile, sheet) {
  d <- readxl::read_excel(xlsfile, sheet = sheet, skip = 2)

  d <-
    d[, grepl(".+", colnames(d))] %>%
    filter(!is.na(M49)) %>%
    select(-`FAO code`, -Area, -`Item Code`, -Item, -`Element Code`, -Element, -Unit)

  colnames(d) <- sub("^(\\d{4})$", "Y\\1", colnames(d))

  d <- tidyr::gather(d, variable, Value, -M49)


  d <-
    full_join(
      d %>% filter(!grepl("F$", variable)) %>% rename(year = variable) %>% mutate(year = sub("^Y", "", year)),
      d %>% filter(grepl("F$", variable)) %>% rename(year = variable, flag = Value) %>% mutate(year = sub("^Y(\\d{4})F$", "\\1", year)),
      by = c("M49", "year")
    ) %>%
    filter(!is.na(Value))

  d <-
    d %>%
    mutate(
      flag =
        case_when(
          .$year %in% as.character(1961:2013) & is.na(.$flag) ~ ",p",
          .$year %in% as.character(1961:2013) & .$flag == "*" ~ "T,p",
          .$year %in% as.character(1961:2013) & .$flag == "A" ~ "T,p",
          .$year %in% as.character(1961:2013) & .$flag == "F" ~ "E,f",
          !(.$year %in% as.character(1961:2013))              ~ "T,p"
        ),
       measuredElementTrade = NA_character_,
       measuredItemCPC = "F1881"
    ) %>%
    rename(geographicAreaM49 = M49, timePointYears = year) %>%
    tidyr::separate(flag, into = c("flagObservationStatus", "flagMethod")) %>%
    # Only FAOSTAT data
    filter(timePointYears <= 2013)


  return(d)
}


d_imports <-
  load_data(xlsfile = f, sheet = "Import values") %>%
  mutate(measuredElementTrade = "5622") %>%
  setDT()

setcolorder(d_imports, c("geographicAreaM49", "measuredElementTrade", "measuredItemCPC",
              "timePointYears", "Value", "flagObservationStatus", "flagMethod"))


d_exports <-
  load_data(xlsfile = f, sheet = "Export values") %>%
  mutate(measuredElementTrade = "5922") %>%
  setDT()

setcolorder(d_exports, c("geographicAreaM49", "measuredElementTrade", "measuredItemCPC",
              "timePointYears", "Value", "flagObservationStatus", "flagMethod"))


d_upload <- bind_rows(d_imports, d_exports)


out <- list()
k <- 0
for (country in unique(d_upload$geographicAreaM49)) {
  print(k <- k + 1) ; flush.console()
  out[[k]] <- SaveData("trade", "total_trade_cpc_m49", d_upload[geographicAreaM49 == country])
  if (nrow(stats$warnings) > 0) {
    stop("XXX")
  }
}

