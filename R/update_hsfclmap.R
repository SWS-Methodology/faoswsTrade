library(faosws)
library(dplyr)
library(readr)

map_file <- 'C:/Users/mongeau/tmp/to_map.csv'


if (CheckDebug()) {
  library(faoswsModules)
  settings_file <- "modules/complete_tf_cpc/sws.yml"
  SETTINGS = faoswsModules::ReadSettings(settings_file)

  ## Define where your certificates are stored
  SetClientFiles(SETTINGS[["certdir"]])

  ## Get session information from SWS.
  ## Token must be obtained from web interface
  GetTestEnvironment(baseUrl = SETTINGS[["server"]],
                     token   = SETTINGS[["token"]])
}


fcl_codes <- ReadDatatable('fcl_2_cpc')$fcl %>%
  as.numeric()


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
  filter(!is.na(year), !is.na(reporter_fao), !is.na(hs))


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



hsfclmap3 <- tbl_df(ReadDatatable("hsfclmap3"))

if (nrow(hsfclmap3) == 0) {
  stop('A problem occurred when fetching the map table. Please, try again.')
}



# Raise warning if countries were NOT in mapping.

if (length(setdiff(unique(add_map$reporter_fao), hsfclmap3$area)) > 0) {
  warning('Some countries were not in the mapping.')
}


updated_map <-
  add_map %>%
  filter(!is.na(fcl)) %>%
  arrange(reporter_fao, flow, hs, year) %>%
  mutate(
    hs = ifelse(
           hs_chap < 10 & stringr::str_sub(hs, 1, 1) != '0',
           paste0('0', formatC(hs, format = 'fg')),
           formatC(hs, format = 'fg')
         ) 
  ) %>%
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

max_record <- max(hsfclmap3$recordnumb)

updated_map$recordnumb <- max_record:(max_record+nrow(updated_map)-1)

