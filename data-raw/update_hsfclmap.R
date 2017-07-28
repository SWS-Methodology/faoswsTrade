#library(faosws)
library(dplyr)
library(readr)
library(readxl)
library(faosws)
library(bit64)

if(CheckDebug()){
  library(faoswsModules)
  SETTINGS = ReadSettings("modules/complete_tf_cpc/sws.yml")
  ## Define where your certificates are stored
  faosws::SetClientFiles(SETTINGS[["certdir"]])
  ## Get session information from SWS. Token must be obtained from web interface
  GetTestEnvironment(baseUrl = SETTINGS[["server"]],
                     token = SETTINGS[["token"]])
}


# Save current options (will be reset at the end)
old_options <- options()

options(scipen = 999)

#hs6standard_file <- 'https://github.com/SWS-Methodology/faoswsTrade/blob/master/data-raw/HS2012-6%20digits%20Standard.xls?raw=true'
hsfclmap3 <- tbl_df(ReadDatatable("hsfclmap3"))

fcl_codes <- as.numeric(tbl_df(faosws::ReadDatatable(table = 'fcl_2_cpc'))$fcl)

add_map <- tbl_df(ReadDatatable('hsfclmap4')) %>%
  filter(!is.na(year), !is.na(reporter_fao), !is.na(hs)) %>%
  mutate(
    hs = ifelse(
           hs_chap < 10 & stringr::str_sub(hs, 1, 1) != '0',
           paste0('0', formatC(hs, format = 'fg')),
           formatC(hs, format = 'fg')
         ) 
  ) %>%
  arrange(reporter_fao, flow, hs, year)


## XXX change some FCL codes that are not valid
add_map <- add_map %>%
  mutate(fcl = ifelse(fcl == 389, 390, fcl)) %>%
  mutate(fcl = ifelse(fcl == 654, 653, fcl))

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
  warning('Removing duplicate HS codes by reporter/year/flow.')
  
  add_map <- add_map %>%
    group_by(reporter_fao, year, flow, hs) %>%
    mutate(n = n(), i = 1:n(), hs_ext_perc = sum(!is.na(hs_extend))/n()) %>%
    ungroup() %>%
    # Prefer cases where hs_extend is available
    filter(hs_ext_perc == 0 | (hs_ext_perc > 0 & !is.na(hs_extend) & n == 1L)) %>%
    select(-n, -i, -hs_ext_perc)
}

#hsfclmap3 <- tbl_df(ReadDatatable("hsfclmap3"))

#tmp_file <- paste0(tempfile(), '.Rdata')
#writeBin(httr::GET(hsfclmap3_file)$content, tmp_file)
#load(tmp_file)


if (nrow(hsfclmap3) == 0) {
  stop('A problem occurred when fetching the map table. Please, try again.')
}



# Raise warning if countries were NOT in mapping.

if (length(setdiff(unique(add_map$reporter_fao), hsfclmap3$area)) > 0) {
  warning('Some countries were not in the original mapping.')
}

#tmp_file <- paste0(tempfile(), '.xls')
#writeBin(httr::GET(hs6standard_file)$content, tmp_file)
#hs6standard <- read_excel(tmp_file, sheet = 'Standard_HS12')

#hs6standard_uniq <-
#  hs6standard %>%
#  group_by(HS2012Code) %>%
#  mutate(n = n()) %>%
#  ungroup() %>%
#  filter(n == 1) %>%
#  mutate(
#    hs6details = 'Standard_HS12',
#    hs6description = paste('FaoStatName', FaoStatName, sep = ': ')
#  ) %>%
#  select(HS2012Code, FaoStatCode, hs6details, hs6description)


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
      #tl_description = `TL description (if available)`
      tl_description = tl_description
    )
}

manual_updated <-
  add_map %>%
  filter(!is.na(fcl))

#auto_updated <-
#  add_map %>%
#  #filter(is.na(fcl), is.na(details), is.na(`TL description (if available)`)) %>%
#  filter(is.na(fcl), is.na(details), is.na(tl_description)) %>%
#  mutate(hs6 = stringr::str_sub(hs, 1, 6)) %>%
#  left_join(
#    hs6standard_uniq,
#    by = c('hs6' = 'HS2012Code')
#  ) %>%
#  filter(!is.na(FaoStatCode)) %>%
#  #mutate(fcl = FaoStatCode, details = hs6details, `TL description (if available)` = hs6description) %>%
#  mutate(fcl = FaoStatCode, details = hs6details, tl_description = hs6description) %>%
#  select(-hs6, -FaoStatCode, -hs6details, -hs6description)
#
#mapped <- bind_rows(manual_updated, auto_updated)
mapped <- manual_updated

unmapped <- anti_join(add_map, mapped, by = c('year', 'reporter_fao', 'flow', 'hs'))

mapped <- adapt_map_sws_format(mapped)

max_record <- max(hsfclmap3$recordnumb)

mapped$recordnumb <- (max_record+1):(max_record+nrow(mapped))

mapped <- mapped %>%
  select(-details, -tl_description) %>%
  mutate(
    fcl      = as.numeric(fcl),
    fromcode = gsub(' ', '', fromcode),
    tocode   = gsub(' ', '', tocode)
  )

# Restore changed options
options(old_options)

