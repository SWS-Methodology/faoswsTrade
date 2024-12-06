

################################################################################
#
# Type: plugin.
#
# Name: ti_BTD.
#
# Description: R script to calculate the Bilateral Trade Discrepancy index (BTD)
#              starting from elements 5610 (Import Quantity) and 5910 (Export
#              Quantity).
#              The following is a brief description of the indicator.
#              BTD: provides a general understanding of the tendencies of
#                   under-reporting or over-reporting in international trade
#                   values between countries. The magnitude of the discrepancy
#                   serves as an indicator for the severity of the issue. The
#                   BTD has two forms for when the reporter country acts either
#                   as an importing or as an exporting country.
#
# Author's email: riccardo.giubilei@fao.org
#
################################################################################



# Plugin initialization ---------------------------------------------------

# Plugin name
plugin_name <- 'ti_BTD'

# Print starting message
message(paste("The", plugin_name, "plugin is running."))



# Packages ----------------------------------------------------------------

# Print packages-loading message
message(paste("The", plugin_name, "plugin is loading packages."))

# Load packages
suppressMessages({
  library(faosws)
  library(faoswsUtil)
  library(faoswsFlag)
  library(data.table)
})


# Environment -------------------------------------------------------------

if (faosws::CheckDebug()) {

  # Connect to .yml file
  library(faoswsModules)
  SETT <- faoswsModules::ReadSettings("sws.yml")

  # Set up offline environment
  server <- SETT[["server"]]
  token <- SETT[["token"]]
  faosws::GetTestEnvironment(baseUrl = server,
                             token = token)

  # # Load helper functions
  # source('R/plugin_helper_functions.R')

}

# Always source files in R/ (useful for local runs).
# Sourcing files is different in git renv context.
# please use the following
### from here
if (CheckDebug()) {
  # setwd(wd)
  files = dir("./R", full.names = TRUE)
} else{
  path <- Sys.getenv('ROOT_PATH')
  files <-
    dir(paste(path, "./R" , sep = "/"), full.names = TRUE)
}

invisible(sapply(files, source))


# Import datatables -------------------------------------------------------

# Print datatable-importing message
message(paste("The", plugin_name, "plugin is importing datatables."))


### Key commodities datatable and two aggregates ###

# Get item codes from key commodities datatable
cpc_codes <- faosws::ReadDatatable(table = "key_commodities_codes")$commodity_code


### Reporter countries ###

# Import reporter countries datatable
reporters_by_year <- faosws::ReadDatatable(table = "reporters_by_year_new_version")

# Get GeographicArea codes from reporter countries
m49_reporters <- m49_partners <- unique(reporters_by_year$m49)

# Get years from reporter countries
reporters_cols <- colnames(reporters_by_year)
years_cols <- grep('year_', reporters_cols, value = TRUE)
years_reporters <- sub('year_', '', years_cols)

# Exclude years for which there are no observations
na_number <- colSums(is.na(reporters_by_year))
empty_cols <- reporters_cols[na_number == nrow(reporters_by_year)]
NA_years_reporters <- sub('year_', '', empty_cols)
years_reporters <- setdiff(years_reporters,
                           NA_years_reporters)
# NB: this is not strictly necessary as countryReports() already excludes these
#     years, but it allows to download the minimal set of data that is required.

# NB: years can be possibly further refined using input parameters.



# Input parameters --------------------------------------------------------

# Print input parameters message
message(paste("The", plugin_name, "plugin is reading the input parameters."))

### Intermediate quantities
intermediate <- swsContext.computationParams$intermediate_quantities


### Years ###

# Retrieve parameters' values
starting_year <- swsContext.computationParams$starting_year
ending_year <- swsContext.computationParams$ending_year

# If NULL, replace them with years from reporter countries datatable
if (is.null(starting_year)) starting_year <- min(years_reporters)
if (is.null(ending_year)) ending_year <- max(years_reporters)

# Check that the starting year does not come after the ending year
stopifnot("The starting year cannot come after the ending year." =
            starting_year <= ending_year)

# Compute input year-range
input_years <- starting_year:ending_year

# Final set of years as intersection between reporters and user interface input
selected_years <- intersect(years_reporters,
                            input_years)


### Query-only parameter ###

# Retrieve parameter's value
query_only <- swsContext.computationParams$query_only

# If query_only is 'yes', retrieve input keys from the query
if (query_only == 'yes') {

  # Input keys
  query_geo_repo <- query_keys('geographicAreaM49Reporter')
  query_geo_part <- query_keys('geographicAreaM49Partner')
  query_elem <- query_keys('measuredElementTrade')
  query_item <- query_keys('measuredItemCPC')
  query_years <- query_keys('timePointYears')
  # N.B.: query_elem is only used to choose which results to return; the others
  #       are used to get input data, and will override the three corresponding
  #       vectors of keys.

  # Replace m49_codes with the query_geo that are in m49_codes (reporter
  # countries), if any; otherwise, throw warning and use the original m49_codes
  if (any(query_geo_repo %in% m49_reporters)) {
    m49_reporters <- query_geo_repo[query_geo_repo %in% m49_reporters]
  } else {
    warning('The queried key(s) for geographicAreaM49Reporter is not present among the reporter countries. All the reporter countries will be considered instead.')
  }
  if (any(query_geo_part %in% m49_partners)) {
    m49_partners <- query_geo_part[query_geo_part %in% m49_partners]
  } else {
    warning('The queried key(s) for geographicAreaM49Partner is not present among the reporter countries. All the reporter countries will be considered instead.')
  }

  # Replace cpc_codes with the query_item that are in cpc_codes (key
  # commodities), if any; otherwise, throw warning and use the original cpc_codes
  if (any(query_item %in% cpc_codes)) {
    cpc_codes <- query_item[query_item %in% cpc_codes]
  } else {
    warning('The queried key(s) for measuredItemCPC is not present among the key commodities. All the key commodities will be considered instead.')
  }

  # Replace selected_years with the query_years that are in selected_years, if
  # any; otherwise, throw warning and use the original selected_years
  if (any(query_years %in% selected_years)) {
    selected_years <- query_years[query_years %in% selected_years]
  } else {
    warning('The queried key(s) for timePointsYears is not included in the years specified when running the plugin. The latter are used instead.')
  }

}



# Import data -------------------------------------------------------------

# Print input data-importing message
message(paste("The", plugin_name, "plugin is importing input data."))

# Trade flows dataset: Import Quantity (5610) and Export Quantity (5910)
data_bilateral <- sws2dataset(dataset = 'completed_tf_cpc_m49',
                              keys = list(geo1 = m49_reporters,
                                          geo2 = m49_partners,
                                          elem = c('5610', '5910'),
                                          item = cpc_codes,
                                          years = selected_years),
                              elem_colnames = c('import', 'export'))



# BTD indicator -----------------------------------------------------------

# Print indicator-calculation message
message(paste("The", plugin_name, "plugin is computing the indicators."))

# Create swapped version of data_bilateral
data_bilateral_swapped <- data_bilateral
colnames(data_bilateral_swapped)[1:2] <- colnames(data_bilateral)[2:1]

# Merge original and swapped data_bilateral to get MBA, EAB, MAB, EBA
data_btd <- merge(data_bilateral,
                  data_bilateral_swapped,
                  suffixes = c('A', 'B'))

# Filter out years for which either country does not report
is.reporterA <- countryReports(countries = data_btd$geographicAreaM49Reporter,
                              years = data_btd$timePointYears)
is.reporterB <- countryReports(countries = data_btd$geographicAreaM49Partner,
                               years = data_btd$timePointYears)
data_btd <- data_btd[is.reporterA & is.reporterB]

# Bilateral trade discrepancy differences and indices
data_btd[, btd_importA_diff := importA - exportB]
data_btd[, btd_importA := (importA - exportB) /  importA]
data_btd[, btd_exportA_diff := importB - exportA]
data_btd[, btd_exportA := (importB - exportA) /  importB]

# Make cases where the denominator is zero return NA
data_btd[importA == 0, 'btd_importA'] <- NA_real_
data_btd[importB == 0, 'btd_exportA'] <- NA_real_



# Save back the results and finalize --------------------------------------

# Print save message
message(paste("The", plugin_name, "plugin is saving the results back to the session."))

# If query_elem does not exist (happens when query_only == 'no'), initialize it
if (isFALSE(exists('query_elem'))) query_elem <- '0'

# Throw warning if none of the indicators is included in the queried elements
select_elem <- intersect(c('504.01', '504.02'),
                         query_elem)
nelem <- length(select_elem)
if (nelem == 0)
  warning('The indicators are not included in the queried key(s) for measuredElementTrade. Both of them will be returned.')

# Check which indicator(s) should be computed
compute_btd_import <- if ('504.01' %in% select_elem | nelem == 0) TRUE else FALSE
compute_btd_export <- if ('504.02' %in% select_elem | nelem == 0) TRUE else FALSE

# BTD
if (isTRUE(compute_btd_import)) {

  # Save the results back to the session
  btd_import_save <-
    faosws::SaveData(domain = 'trade',
                     dataset = 'trade_indicators_bilateral',
                     data = data_btd[, .(geographicAreaM49Reporter = geographicAreaM49Reporter,
                                         geographicAreaM49Partner = geographicAreaM49Partner,
                                         measuredItemCPC = measuredItemCPC,
                                         timePointYears = timePointYears,
                                         Value = btd_importA,
                                         measuredElementTrade = '504.01',
                                         flagObservationStatus = fifelse(is.na(get('btd_importA')),
                                                                         NA_character_,
                                                                         'E'),
                                         flagMethod = fifelse(is.na(get('btd_importA')),
                                                              NA_character_,
                                                              'i'))],
                     waitTimeout = 100000)

  # Log
  base_string <- paste("Log for the BTD indicator - Importing reporter country:",
                       "%d written observations, %d appended, %d ignored, %d discarded.")
  message(do.call(sprintf, c(base_string,
                             lapply(btd_import_save[1:4], FUN = identity))))

}

# BTD
if (isTRUE(compute_btd_export)) {

  btd_export_save <-
    faosws::SaveData(domain = 'trade',
                     dataset = 'trade_indicators_bilateral',
                     data = data_btd[, .(geographicAreaM49Reporter = geographicAreaM49Reporter,
                                         geographicAreaM49Partner = geographicAreaM49Partner,
                                         measuredItemCPC = measuredItemCPC,
                                         timePointYears = timePointYears,
                                         Value = btd_exportA,
                                         measuredElementTrade = '504.02',
                                         flagObservationStatus = fifelse(is.na(get('btd_exportA')),
                                                                         NA_character_,
                                                                         'E'),
                                         flagMethod = fifelse(is.na(get('btd_exportA')),
                                                              NA_character_,
                                                              'i'))],
                     waitTimeout = 100000)

  # Log
  base_string <- paste("Log for the BTD indicator - Exporting reporter country:",
                       "%d written observations, %d appended, %d ignored, %d discarded.")
  message(do.call(sprintf, c(base_string,
                             lapply(btd_export_save[1:4], FUN = identity))))

}

# Intermediate quantities
if (intermediate == 'yes') {

  # Save back d1 only if it is included in the query or if 01 is calculated
  if ('504.d1' %in% select_elem | isTRUE(compute_btd_import)) {

    # Save the results back to the session
    btd_Mdiff_save <-
      faosws::SaveData(domain = 'trade',
                       dataset = 'trade_indicators_bilateral',
                       data = data_btd[, .(geographicAreaM49Reporter = geographicAreaM49Reporter,
                                           geographicAreaM49Partner = geographicAreaM49Partner,
                                           measuredItemCPC = measuredItemCPC,
                                           timePointYears = timePointYears,
                                           Value = btd_importA_diff,
                                           measuredElementTrade = '504.d1',
                                           flagObservationStatus = fifelse(is.na(get('btd_importA_diff')),
                                                                           NA_character_,
                                                                           'E'),
                                           flagMethod = fifelse(is.na(get('btd_importA_diff')),
                                                                NA_character_,
                                                                'i'))],
                       waitTimeout = 100000)
  }

  # Save back d1 only if it is included in the query or if 01 is calculated
  if ('504.d1' %in% select_elem | isTRUE(compute_btd_import)) {

    btd_Xdiff_save <-
      faosws::SaveData(domain = 'trade',
                       dataset = 'trade_indicators_bilateral',
                       data = data_btd[, .(geographicAreaM49Reporter = geographicAreaM49Reporter,
                                           geographicAreaM49Partner = geographicAreaM49Partner,
                                           measuredItemCPC = measuredItemCPC,
                                           timePointYears = timePointYears,
                                           Value = btd_exportA_diff,
                                           measuredElementTrade = '504.d2',
                                           flagObservationStatus = fifelse(is.na(get('btd_exportA_diff')),
                                                                           NA_character_,
                                                                           'E'),
                                           flagMethod = fifelse(is.na(get('btd_exportA_diff')),
                                                                NA_character_,
                                                                'i'))],
                       waitTimeout = 100000)

  }

}

# Print information
#print(sessionInfo())
#print(version)

# Print ending message
message(paste("Process completed successfully!"))

