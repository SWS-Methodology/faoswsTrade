

################################################################################
#
# Type: plugin.
#
# Name: ti_SAE_ATO.
#
# Description: R script to calculate the Share of Agricultural Exports to GDP
#              (SAE) and the Agricultural Trade Openness index (ATO) starting
#              from elements 5622 (Import Value), 5922 (Export Value), and 8003
#              (GDP). The following is a brief description of the indicators.
#              SAE: shows the overall contribution of agricultural exports to
#                   GDP.
#              ATO: measures the importance of international trade in goods
#                   relative to the domestic economic output of an economy.
#
# Author's email: riccardo.giubilei@fao.org
#
################################################################################



# Plugin initialization ---------------------------------------------------

# Plugin name
plugin_name <- 'ti_SAE_ATO'

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

# Print input prameters message
message(paste("The", plugin_name, "plugin is reading the input parameters."))

### Input domain ###

# Retrieve parameters' values
trade_dataset <- swsContext.computationParams$trade_source


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

# Trade dataset: Import Value (5622) and Export Value (5922) of agricultural
#                products F1881 and F1882
data_agrtrade <- sws2dataset(dataset = trade_dataset,
                             keys = list(geo = m49_codes,
                                         elem = c('5622', '5922'),
                                         item = c('F1881', 'F1882'),
                                         years = selected_years),
                             elem_colnames = c('import', 'export'))

# Macro statistics complete dataset: GDP (8003)
data_gdp <- sws2dataset(dataset = 'ess_eco_macroind_complete',
                        keys = list(geo = m49_codes,
                                    elem = '8003',
                                    years = selected_years),
                        elem_colnames = 'gdp')



# SAE and ATO indicators --------------------------------------------------

# Print indicator-calculation message
message(paste("The", plugin_name, "plugin is computing the indicators."))

# Merge the input datasets
data_saeato <- merge(x = data_agrtrade,
                     y = data_gdp,
                     all = T)

# Filter out years for which countries do not report
is.reporter <- countryReports(countries = data_saeato$geographicAreaM49,
                              years = data_saeato$timePointYears)
data_saeato <- data_saeato[is.reporter]

# Share of Agricultural Exports to GDP
data_saeato[, sae := export / (gdp * 1000) * 100]
# GDP is in MUNIT, export in KUNIT -> GDP multiplied by 1000

# Agricultural Trade Openness Index
data_saeato[, ato := (import + export) / (gdp * 1000)]
# GDP is in MUNIT, export in KUNIT -> GDP multiplied by 1000

# Make all-zero cases return NA
#data_saeato[gdp == 0 & import == 0 & export == 0, c('sae', 'ato')] <- NA_real_
# No all-zero cases here, and no cases where GDP is zero.



# Save back the results and finalize --------------------------------------

# Print save message
message(paste("The", plugin_name, "plugin is saving the results back to the session."))

# If query_elem does not exist, initialize it
if (isFALSE(exists('query_elem'))) query_elem <- '0'

# Throw warning if none of the indicators is included in the queried elements
select_elem <- intersect(c('503', '505'),
                         query_elem)
nelem <- length(select_elem)
if (nelem == 0)
  warning('The indicators are not included in the queried key(s) for measuredElementTrade. Both of them will be returned.')

# Check which indicator(s) should be computed
compute_sae <- if ('503' %in% select_elem | nelem == 0) TRUE else FALSE
compute_ato <- if ('505' %in% select_elem | nelem == 0) TRUE else FALSE

# SAE
if (isTRUE(compute_sae)) {

  # Save the results back to the session
  sae_save <-
    faosws::SaveData(domain = 'trade',
                     dataset = 'trade_indicators',
                     data = data_saeato[, .(geographicAreaM49 = geographicAreaM49,
                                            measuredItemCPC = measuredItemCPC,
                                            timePointYears = timePointYears,
                                            Value = sae,
                                            measuredElementTrade = '503',
                                            flagObservationStatus = fifelse(is.na(get('sae')),
                                                                            NA_character_,
                                                                            'E'),
                                            flagMethod = fifelse(is.na(get('sae')),
                                                                 NA_character_,
                                                                 'i'))],
                     waitTimeout = 100000)

  # Log
  base_string <- paste("Log for the SAE indicator:",
                       "%d written observations, %d appended, %d ignored, %d discarded.")
  message(do.call(sprintf, c(base_string,
                             lapply(sae_save[1:4], FUN = identity))))

}

# ATO
if (isTRUE(compute_ato)) {

  # Save the results back to the session
  ato_save <-
    faosws::SaveData(domain = 'trade',
                     dataset = 'trade_indicators',
                     data = data_saeato[, .(geographicAreaM49 = geographicAreaM49,
                                            measuredItemCPC = measuredItemCPC,
                                            timePointYears = timePointYears,
                                            Value = ato,
                                            measuredElementTrade = '505',
                                            flagObservationStatus = fifelse(is.na(get('ato')),
                                                                            NA_character_,
                                                                            'E'),
                                            flagMethod = fifelse(is.na(get('ato')),
                                                                 NA_character_,
                                                                 'i'))],
                     waitTimeout = 100000)

  # Log
  base_string <- paste("Log for the ATO indicator:",
                       "%d written observations, %d appended, %d ignored, %d discarded.")
  message(do.call(sprintf, c(base_string,
                             lapply(ato_save[1:4], FUN = identity))))

}

# Print information
#print(sessionInfo())
#print(version)

# Print ending message
message(paste("Process completed successfully!"))

