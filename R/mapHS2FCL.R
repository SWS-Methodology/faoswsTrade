#' Umbrella function to map HS codes to FCL codes
#'
#' The function takes trade data set with mapping table and finds corresponding
#' FCL codes for specific reporter, flow and HS code combinations.
#'
#' @param tradedata Trade data frame. Columns reporter, flow and hs are expected.
#' @param maptable Mapping table.
#' @param hs6maptable HS6 mapping table.
#' @param year Integer, year the trade module run on.
#' @param parallel Logical, should multicore backend be used. False by default.
#'
#' @return Data frame with unique combinations reporter/flow/hs/fcl.
#'
#' @import dplyr
#' @export

mapHS2FCL <- function(tradedata,
                      maptable,
                      hs6maptable,
                      year = NULL,
                      parallel = FALSE) {

  stopifnot(!is.null(year))
  stopifnot(is.integer(year))

  # Name for passing to reporting functions
  tradedataname <- lazyeval::expr_text(tradedata)

  # HS6 mapping table subset with 1-to-1 hs->fcl links
  hs6maptable <- hs6maptable %>%
    filter_(~fcl_links == 1L)

  flog.trace("HS+ mapping: extraction of unique combinations", name = "dev")

  uniqhs <- tradedata %>%
    select_(~reporter, ~flow, ~hs6, ~hs) %>%
    distinct

  # Drop records mapped with hs6
  uniqhs <- anti_join(uniqhs, hs6maptable,
                      by = c("reporter", "flow", "hs6"))

  # Reports full table in the text report and as csv file
  # XXX: bring back
  #rprt_uniqhs(uniqhs, tradedataname = tradedataname)

  # Drop mapping records already used in hs6 mapping
  # maptable <- anti_join(maptable, hs6maptable,
  #                       by = c("area" = "reporter", "flow", "hs6"))

  flog.trace("HS+ mapping: align HS codes from data and table", name = "dev")

  hslength <- maxHSLength(uniqhs, maptable)

  # Reports full table in the text report and as csv file
  # XXX: bring back
  #rprt_hslength(hslength, tradedataname = tradedataname)

  uniqhs <- uniqhs %>%
    left_join(hslength, by = c("reporter", "flow")) %>%
    dplyr::mutate_(hsextchar = ~stringr::str_pad(hs,
                                          width = maxhslength,
                                          side = "right",
                                          pad = "0"),
            hsext = ~as.numeric(hsextchar))

  # Reports full table in the text report and as csv file
  # XXX: bring back
  #rprt_hschanged(uniqhs, tradedataname = tradedataname)

  maptable <- hslength %>%
    left_join(maptable, by = c("reporter" = "area", "flow")) %>%
    # XXX this happens, e.g., for (removed some columns):
    # reporter flow maxhslength fromcode   tocode fcl startyear endyear recordnumb
    #       97    2           8 01031000 01023999 946      2012    2050    3674617
    #       97    2           8 01040000 01029099 866      2012    2050    3674616
    #      174    1           8 19053320 19053299 110      2012    2050    1779049
    dplyr::mutate(
      from_gt_to = as.numeric(fromcode) > as.numeric(tocode),
      fromcode = ifelse(from_gt_to, tocode, fromcode),
      tocode   = ifelse(from_gt_to, fromcode, tocode)
    ) %>%
    dplyr::select(-from_gt_to) %>%
    dplyr::mutate_(fromcodeextchar = ~stringr::str_pad(fromcode,
                                         width = maxhslength,
                                         side = "right",
                                         pad = "0"),
            tocodeextchar = ~stringr::str_pad(tocode,
                                       width = maxhslength,
                                       side = "right",
                                       pad = "9")) %>%
    dplyr::mutate_(fromcodeext = ~as.numeric(fromcodeextchar),
            tocodeext   = ~as.numeric(tocodeextchar))

  # XXX: bring back
  #rprt_map_hschanged(maptable, tradedataname = tradedataname)

  flog.trace("HS+ mapping: looking for links", name = "dev")
  uniqhs <- hsInRange(uniqhs, maptable, parallel = parallel)

  hs2fcl_mapped_links <- uniqhs
  # XXX: bring back
  #rprt_writetable(hs2fcl_mapped_links, prefix = tradedataname)

  # Report on nolinks
  # XXX: bring back
  #rprt_hs2fcl_nolinks(uniqhs, tradedataname = tradedataname)

  # Report on multilinks
  # XXX: bring back
  #rprt_hs2fcl_multilinks(uniqhs, tradedataname = tradedataname)

  flog.trace("HS+ mapping: selection from multiple matches", name = "dev")

  uniqhs <- sel1FCL(uniqhs, maptable, cur_yr = year)

  uniqhs
}
