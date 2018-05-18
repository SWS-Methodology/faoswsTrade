#' Create HS6->CPC mapping table.
#'
#' @import dplyr
#' @import stringr
#' @import futile.logger
#'
#' @param maptable hscpcmap data frame.
#' @return Data frame with columns reporter, flow, hs6, cpc
#' @export
#'

extract_hs6cpcmap <- function(maptable = NULL) {

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
                      ~cpc,
                      ~fcl)

  # Convert hs columns to integer hs6 and
  # calculate from-to range
  flog.trace("HS6 map: calculation of HS ranges", name = "dev")
  maptable <- maptable %>%
    dplyr::mutate_at(vars(ends_with("code")),
              funs(str_sub(., end = 6L))) %>%
    dplyr::mutate_at(vars(ends_with("code")),
              as.integer) %>%
    dplyr::mutate_(hsrange = ~tocode - fromcode)

  # Subset maptable with zero from-to hs range
  # where we don't need to add intermediate codes
  maptable_0range <- maptable %>%
    filter_(~hsrange == 0) %>%
    select_(~reporter,
            ~flow,
            hs6 = ~fromcode,
            ~cpc,
            ~fcl)

  # Map table subset where real hs from-to range exists
  # and we need to fill numbers. I.e., the range is expanded
  # as a vector and each element of this vector is coupled
  # with the CPC valid for the range where the HS comes from
  flog.trace("HS6 map: convert HS ranges into explicit HS codes",
             name = "dev")

  maptable_range <- maptable %>%
    filter_(~hsrange > 0) %>%
    rowwise() %>%
    dplyr::mutate(hs6 = list(fromcode:tocode)) %>%
    tidyr::unnest() %>%
    select(reporter, flow, hs6, cpc, fcl)

  # Bind both subsets and then calculate number of matching
  # cpc codes per each hs6
  flog.trace("HS6 map: counting CPC matches per HS6", name = "dev")

  ## For some reason n_distinct() is very slow on strings (cpc),
  ## while it worked fine with numbers (fcl). The original
  ## implementation below, while now an approach that does not
  ## use n_distinct() is used (anyway, it takes ~ 30% more time
  ## (but we are talking about something that takes less than 1 min)
  #bind_rows(maptable_0range, maptable_range) %>%
  #group_by(reporter, flow, hs6) %>%
  #dplyr::mutate(cpc_links = n_distinct(cpc)) %>%
  #ungroup() %>%
  #distinct()
  bind_rows(maptable_0range, maptable_range) %>%
  group_by(reporter, flow, hs6, cpc, fcl) %>%
  distinct() %>%
  group_by(reporter, flow, hs6) %>%
  mutate(cpc_links = n()) %>%
  ungroup()
}

