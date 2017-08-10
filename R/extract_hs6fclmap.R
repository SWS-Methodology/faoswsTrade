#' Create HS6->FCL mapping table.
#'
#' @import dplyr
#' @import stringr
#' @import futile.logger
#'
#' @param maptable hsfclmap data frame.
#' @param parallel Should parallel execution be used if available. FALSE by
#'   default.
#' @return Data frame with columns reporter, flow, hs6, fcl
#' @export
#'

extract_hs6fclmap <- function(maptable = NULL, parallel = FALSE) {

  stopifnot(!is.null(maptable))

  # Rename area column to reporter as in future we want to
  # use reporter in the mapping table (there is an issue at github)
  if(!"reporter" %in% colnames(maptable) &
     "area" %in% colnames(maptable)) {
    maptable <- rename_(maptable, .dots = list(reporter = ~area))
  }

  # Drop garbage
  maptable <- select_(maptable,
                      ~reporter,
                      ~flow,
                      ~fromcode,
                      ~tocode,
                      ~fcl)

  # Convert hs columns to integer hs6 and
  # calculate from-to range
  flog.trace("HS6 map: calculation of HS ranges", name = "dev")
  maptable <- maptable %>%
    mutate_at(vars(ends_with("code")),
              funs(str_sub(., end = 6L))) %>%
    mutate_at(vars(ends_with("code")),
              as.integer) %>%
    mutate_(hsrange = ~tocode - fromcode)

  # Subset maptable with zero from-to hs range
  # where we don't need to add intermediate codes
  maptable_0range <- maptable %>%
    filter_(~hsrange == 0) %>%
    select_(~reporter,
            ~flow,
            hs6 = ~fromcode,
            ~fcl)

  # Map table subset where real hs from-to range exists
  # and we need to fill numbers. I.e., the range is expanded
  # as a vector and each element of this vector is coupled
  # with the FCL valid for the range where the HS comes from
  flog.trace("HS6 map: convert HS ranges into explicit HS codes",
             name = "dev")
  maptable_range <- maptable %>%
    filter_(~hsrange > 0) %>%
    rowwise() %>%
    mutate(hs6 = list(fromcode:tocode)) %>%
    tidyr::unnest() %>%
    select(reporter, flow, hs6, fcl)

  # Bind both subsets and then calculate number of matching
  # fcl codes per each hs6
  flog.trace("HS6 map: counting FCL matches per HS6", name = "dev")
  bind_rows(maptable_0range, maptable_range) %>%
    arrange_(~reporter, ~flow, ~hs6, ~fcl) %>%
    distinct() %>%
    # Split by chunks to be efficient in parallel execution
    plyr::ddply(.variables = c("reporter", "flow"),
                function(df) {
                  df %>%
                    group_by_(~hs6) %>%
                    mutate_(fcl_links = ~length(unique(fcl))) %>%
                    ungroup()
                },
                .parallel = parallel,
                .paropts = list(.packages = "dplyr"))
}
