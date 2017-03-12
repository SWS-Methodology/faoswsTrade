#' A function to add columns with area names to a data frame where reporter or
#' partner columns exist.
#'
#' @param data A data frame where reporter or partner columns exists.
#' @param codeclass A character string indicating classification of area codes:
#' "fao", "m49" or "geonom". By default "fao".
#' @param areacolumns A character vector indicating names of columns with area
#' codes. By default c("reporter", "partner").
#' @return Original data frame with added columns.
#' @export
#' @import dplyr

add_area_names <- function(data, codeclass = "fao",
                           areacolumns = c("reporter", "partner")) {

  codeclass <- tolower(codeclass)

  if(codeclass != "fao") stop("Only FAO area codes are supported now.")

  columns2change <- colnames(data)[colnames(data) %in% areacolumns]

  if(length(columns2change) == 0L) {
    message("No suitable columns detected. Nothing to do.")
    return(data)
  }

  data <- data %>%
    mutate_at(columns2change, funs_(name = paste0("area_name_", codeclass)))

  data

}
