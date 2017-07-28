#' Check which flows for which countries need to be mirrored.
#'
#' Some countries may be listed as partners, but never show up
#' as reporters, while other may have an incomplete coverage of
#' reported flows (e.g., imports are reported, but not exports
#' and partner report having imports from that country). This
#' function checks when one of these two cases happen and build
#' a table of them.
#'
#' @param data Trade dataset with ALL reporters included.
#' @param names Logical: if TRUE an additional column is
#'   added to the result with country names (see
#'   \code{\link[faoswsTrade]{faoAreaName}}; if FALSE (default)
#'   such column is not added.
#' @return A table with two columns: \code{area} is the country
#'   (that could originally have been a reporter or a partner)
#'   code, and \code{flow} that is an integer representing imports
#'   (1) or exports (2).
#' @import dplyr
#' @export

flowsToMirror <- function(data = NA, names = FALSE) {

  if (missing(data)) stop('"data" is missing.')

  if (nrow(data) == 0) stop('"data" has no rows.')

  if (!is.logical(names)) stop('"names" must either TRUE or FALSE.')

  # Number of flows
  N <- length(unique(data$flow))

  res <- bind_rows(
      data %>%
        count(reporter, flow) %>%
        rename(area = reporter) %>%
        ungroup(),
      data.frame(
        area  = rep(unique(data$partner), each = N),
        flow  = sort(unique(data$flow)),
        n = 0
      )
    ) %>%
    group_by(area, flow) %>%
    summarise(n = sum(n, na.rm = TRUE)) %>%
    ungroup() %>%
    filter(n == 0) %>%
    select(-n)

  if (names) {
    return(res %>% mutate(name = faoAreaName(area)))
  } else {
    return(res)
  }
}
