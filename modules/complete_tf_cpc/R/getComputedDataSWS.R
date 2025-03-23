#' Get computed dataset on SWS.
#'
#' @param reporter Reporter.
#' @param omit Logical indicating whether to omit or not NAs.
#'
#' @return A data.table with results.
#'
#' @import dplyr
#'
#' @export

getComputedDataSWS <- function(reporter = NA, omit = FALSE) {

  # File with "historical" (validated) data, to avoid re-downloading old data
  f <-
    file.path(
      Sys.getenv("R_SWS_SHARE_PATH"),
      "trade/validation_tool_files/tmp/historical",
      paste0(reporter, ".rds")
    )

  if (file.exists(f)) {
    d_old <- readRDS(f)
  } else {
    d_old <-
      data.table(
        geographicAreaM49     = character(),
        measuredElementTrade  = character(),
        measuredItemCPC       = character(),
        timePointYears        = character(),
        Value                 = numeric(),
        flagObservationStatus = character(),
        flagMethod            = character()
    )
  }

  # TODO: use error handling
  key <- DatasetKey(domain  = 'trade',
                    dataset = 'completed_tf_cpc_m49',
                    dimensions = list(
                      Dimension(name = Vars[['reporters']], keys = reporter),
                      Dimension(name = Vars[['partners']],  keys = Keys[['partners']]),
                      Dimension(name = Vars[['items']],     keys = Keys[['items']]),
                      Dimension(name = Vars[['elements']],  keys = Keys[['elements']]),
                      Dimension(name = Vars[['years']],     keys = setdiff(Keys[['years']], unique(d_old$timePointYears)))))

  d_new <- GetData(key = key, omitna = omit)

  rbind(d_old, d_new)
}

