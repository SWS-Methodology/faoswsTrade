#library(faosws)
library(dplyr)
library(readr)
library(readxl)

map_file <- 'C:/Users/mongeau/tmp/to_map.csv'
fcl_2_cpc_file <- 'C:/Users/mongeau/Dropbox/FAO/datatables/fcl_2_cpc.csv'

hs6standard_file <- 'https://github.com/SWS-Methodology/faoswsTrade/blob/master/data-raw/HS2012-6%20digits%20Standard.xls?raw=true'
hsfclmap3_file <- 'https://github.com/SWS-Methodology/hsfclmap/blob/master/data/hsfclmap3.RData?raw=true'

#if (CheckDebug()) {
#  library(faoswsModules)
#  settings_file <- "modules/complete_tf_cpc/sws.yml"
#  SETTINGS = faoswsModules::ReadSettings(settings_file)
#
#  ## Define where your certificates are stored
#  SetClientFiles(SETTINGS[["certdir"]])
#
#  ## Get session information from SWS.
#  ## Token must be obtained from web interface
#  GetTestEnvironment(baseUrl = SETTINGS[["server"]],
#                     token   = SETTINGS[["token"]])
#}
#
#fcl_codes <- ReadDatatable('fcl_2_cpc')$fcl %>%
#  as.numeric()

fcl_codes <- read_csv(fcl_2_cpc_file)$fcl

add_map <- read_csv(
    map_file,
    col_types = cols(
      `Mapped by` = col_character(),
      year = col_integer(),
      reporter_fao = col_integer(),
      reporter = col_character(),
      flow = col_integer(),
      hs_chap = col_integer(),
      hs = col_double(),
      hs_extend = col_double(),
      fcl = col_integer(),
      details = col_character(),
      `TL description (if available)` = col_character()
    )
  ) %>%
  filter(!is.na(year), !is.na(reporter_fao), !is.na(hs)) %>%
  mutate(
    hs = ifelse(
           hs_chap < 10 & stringr::str_sub(hs, 1, 1) != '0',
           paste0('0', formatC(hs, format = 'fg')),
           formatC(hs, format = 'fg')
         ) 
  ) %>%
  arrange(reporter_fao, flow, hs, year)


# Check that all FCL codes are valid

fcl_diff <- setdiff(unique(add_map$fcl), fcl_codes)

fcl_diff <- fcl_diff[!is.na(fcl_diff)]

if (length(fcl_diff) > 0) {
  stop(paste('Invalid FCL codes:', paste(fcl_diff, collapse = ', ')))
}

# Check that years are in a valid range

if (min(add_map$year) < 2000) {
  stop('The minimum year should not be lower than 2000.')
}

if (max(add_map$year) > as.numeric(format(Sys.Date(), '%Y'))) {
  stop('The maximum year should not be greater than the current year.')
}

# Check that there are no duplicate codes

tmp <- add_map %>%
  count(reporter_fao, year, flow, hs) %>%
  filter(n > 1)

if (nrow(tmp) > 0) {
  stop('There are duplicate HS codes by reporter/year/flow.')
}



#hsfclmap3 <- tbl_df(ReadDatatable("hsfclmap3"))

tmp_file <- paste0(tempfile(), '.Rdata')
writeBin(httr::GET(hsfclmap3_file)$content, tmp_file)
load(tmp_file)


if (nrow(hsfclmap3) == 0) {
  stop('A problem occurred when fetching the map table. Please, try again.')
}



# Raise warning if countries were NOT in mapping.

if (length(setdiff(unique(add_map$reporter_fao), hsfclmap3$area)) > 0) {
  warning('Some countries were not in the mapping.')
}

tmp_file <- paste0(tempfile(), '.xls')
writeBin(httr::GET(hs6standard_file)$content, tmp_file)
hs6standard <- read_excel(tmp_file, sheet = 'Standard_HS12')

hs6standard_uniq <-
  hs6standard %>%
  group_by(HS2012Code) %>%
  mutate(n = n()) %>%
  ungroup() %>%
  filter(n == 1) %>%
  mutate(
    hs6details = 'Standard_HS12',
    hs6description = paste('FaoStatName', FaoStatName, sep = ': ')
  ) %>%
  select(HS2012Code, FaoStatCode, hs6details, hs6description)


adapt_map_sws_format <- function(data) {
  data %>%
    mutate(
      startyear = year,
      endyear = 2050L,
      fromcode = hs,
      tocode = hs,
      recordnumb = NA_integer_
    ) %>%
    select(
      area = reporter_fao,
      flow,
      fromcode,
      tocode,
      fcl,
      startyear,
      endyear,
      recordnumb,
      details,
      tl_description = `TL description (if available)`
    )
}

manual_updated <-
  add_map %>%
  filter(!is.na(fcl))

auto_updated <-
  add_map %>%
  filter(is.na(fcl), is.na(details), is.na(`TL description (if available)`)) %>%
  mutate(hs6 = stringr::str_sub(hs, 1, 6)) %>%
  left_join(
    hs6standard_uniq,
    by = c('hs6' = 'HS2012Code')
  ) %>%
  filter(!is.na(FaoStatCode)) %>%
  mutate(fcl = FaoStatCode, details = hs6details, `TL description (if available)` = hs6description) %>%
  select(-hs6, -FaoStatCode, -hs6details, -hs6description)

mapped <- bind_rows(manual_updated, auto_updated)

unmapped <- anti_join(add_map, mapped, by = c('year', 'reporter_fao', 'flow', 'hs'))

mapped <- adapt_map_sws_format(mapped)

max_record <- max(hsfclmap3$recordnumb)

mapped$recordnumb <- max_record:(max_record+nrow(mapped)-1)


