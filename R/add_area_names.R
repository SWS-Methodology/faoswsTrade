#' A function to add columns with area names to a data frame where reporter or
#' partner columns exist.
#'
#' @param data A data frame where reporter or partner columns exists.
#' @param code_class A character string indicating classification of area codes:
#' "fao", "m49" or "geonom". By default "fao".
#' @param area_columns A character vector indicating names of columns with area
#' codes. By default c("reporter", "partner").
#' @return Original data frame with added columns.
#' @export
#' @import dplyr

add_area_names <- function(data, code_class = "fao",
                           area_columns = c("reporter", "partner")) {

  code_class <- tolower(code_class)

  if(code_class != "fao") stop("Only FAO area codes are supported now.")

  columns2change <- colnames(data)[colnames(data) %in% area_columns]

  if(length(columns2change) == 0L) {
    message("No suitable columns detected. Nothing to do.")
    return(data)
  }

  data <- data %>%
    mutate_at(columns2change, funs_(name = paste0("area_name_", code_class)))

  data

}
