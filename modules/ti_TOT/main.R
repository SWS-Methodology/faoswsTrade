

################################################################################
#
# Type: plugin.
#
# Name: ti_TOT.
#
# Description: R script to calculate the Terms of Trade index (TOT) starting
#              from elements 5630 (Import Quantity) and 5930 (Export Quantity).
#              The following is a brief description of the indicator.
#              TOT: indicates how much the relative price between export and
#                   imports has changed with respect to the base year.
#
# Author's email: riccardo.giubilei@fao.org
#
################################################################################



# Plugin initialization ---------------------------------------------------

# Plugin name
plugin_name <- 'ti_TOT'

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

  # Load helper functions
  # source('plugin_helper_functions.R')

}

# Always source files in R/ (useful for local runs).
# Sourcing files is different in git renv context.
# please use the following
### START
if (CheckDebug()) {
  # setwd(wd)
  files = dir("./R", full.names = TRUE)
} else{
  path <- Sys.getenv('ROOT_PATH')
  files <-
    dir(paste(path, "./R" , sep = "/"), full.names = TRUE)
}

invisible(sapply(files, source))

### END
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
m49_codes <- unique(reporters_by_year$m49)

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
  query_geo <- query_keys('geographicAreaM49')
  query_elem <- query_keys('measuredElementTrade')
  query_item <- query_keys('measuredItemCPC')
  query_years <- query_keys('timePointYears')
  # N.B.: query_elem is only used to choose which results to return; the others
  #       are used to get input data, and will override the three corresponding
  #       vectors of keys.

  # Replace m49_codes with the query_geo that are in m49_codes (reporter
  # countries), if any; otherwise, throw warning and use the original m49_codes
  if (any(query_geo %in% m49_codes)) {
    m49_codes <- query_geo[query_geo %in% m49_codes]
  } else {
    warning('The queried key(s) for geographicAreaM49 is not present among the reporter countries. All the reporter countries will be considered instead.')
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

# Years 2014, 2015, 2016 must be always present as they are used for the base year
base_years <- as.character(2014:2016)
tot_years <- sort(unique(c(base_years, selected_years)))

# Trade dataset: Import Unit Value (5630) and Export Unit Value (5930)
data_trade <- sws2dataset(dataset = 'total_trade_cpc_m49',
                          keys = list(geo = m49_codes,
                                      elem = c('5630', '5930'),
                                      item = cpc_codes,
                                      years = tot_years),
                          elem_colnames = c('import', 'export'))



# TOT indicator -----------------------------------------------------------

# Print indicator-calculation message
message(paste("The", plugin_name, "plugin is computing the indicators."))

## Add two columns: import_base and export_base (three-year averages centered in 2015) ##

# Select only records from base years
only_base <- data_trade[timePointYears %in% base_years, ]

# import_base calculation
only_base[, import := mean(import),
          by = .(geographicAreaM49, measuredItemCPC)]

# export_base calculation
only_base[, export := mean(export),
          by = .(geographicAreaM49, measuredItemCPC)]

# Exclude year column to expand prices to other years when merging
only_base[, timePointYears := NULL]

# Take only unique entries (2014, 2015 and 2016 have now become a single entry)
only_base <- unique(only_base)

# Consider only the years that have been actually queried
data_trade <- data_trade[timePointYears %in% selected_years, ]

# Merge with data_trade
data_tot <- merge(x = data_trade,
                  y = only_base,
                  all = TRUE,
                  suffixes = c('', '_base'))

# Filter out years for which countries do not report
is.reporter <- countryReports(countries = data_tot$geographicAreaM49,
                              years = data_tot$timePointYears)
data_tot <- data_tot[is.reporter]

# Terms of trade index
data_tot[, tot := ((export / import) / (export_base / import_base)) * 100]

# Make cases where the denominator is zero return NA
data_tot[import == 0 | export_base == 0, 'tot'] <- NA_real_



# Save back the results and finalize --------------------------------------

# Print save message
message(paste("The", plugin_name, "plugin is saving the results back to the session."))

# Always compute the TOT as it is the only one indicator here
compute_tot <- TRUE

# TOT
if (isTRUE(compute_tot)) {

  # Save the results back to the session
  tot_save <-
    faosws::SaveData(domain = 'trade',
                     dataset = 'trade_indicators',
                     data = data_tot[, .(geographicAreaM49 = geographicAreaM49,
                                         measuredItemCPC = measuredItemCPC,
                                         timePointYears = timePointYears,
                                         Value = tot,
                                         measuredElementTrade = '506',
                                         flagObservationStatus = fifelse(is.na(get('tot')),
                                                                         NA_character_,
                                                                         'E'),
                                         flagMethod = fifelse(is.na(get('tot')),
                                                              NA_character_,
                                                              'i'))],
                     waitTimeout = 100000)

  # Log
  base_string <- paste("Log for the TOT indicator:",
                       "%d written observations, %d appended, %d ignored, %d discarded.")
  message(do.call(sprintf, c(base_string,
                             lapply(tot_save[1:4], FUN = identity))))

}


# Print information
#print(sessionInfo())
#print(version)

# Print ending message
message(paste("Process completed successfully!"))

