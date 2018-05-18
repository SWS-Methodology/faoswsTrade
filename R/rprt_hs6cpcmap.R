#' Reports on results of extraction of HS6->CPC map
#'
#' @param hs6cpcmap produced by extract_hs6cpcmap function
#' @import dplyr

rprt_hs6cpcmap <- function(hs6cpcmap = NULL) {

  stopifnot(!is.null(hs6cpcmap))

  hs6cpcmap_by_reporter_stat <- hs6cpcmap %>%
    group_by_(~reporter, ~flow) %>%
    dplyr::mutate(total_links = n()) %>%
    group_by_(~total_links, ~hs6, add = TRUE) %>%
    dplyr::summarise(one2one = n() == 1L) %>%
    group_by_(~reporter, ~flow, ~total_links) %>%
    dplyr::summarise_(
      uniq_hs6      = ~n(),
      one2one_count = ~sum(one2one),
      one2one_prop  = ~one2one_count / uniq_hs6
    ) %>%
    ungroup()

  hs6cpcmap_total_stat <- hs6cpcmap_by_reporter_stat %>%
    dplyr::summarise_at(vars(total_links, uniq_hs6, one2one_count), funs(sum)) %>%
    dplyr::mutate_(one2one_prop = ~one2one_count / uniq_hs6)

  hs6cpcmap_by_reporter_stat <- add_area_names(hs6cpcmap_by_reporter_stat, "fao")

  rprt_writetable(hs6cpcmap_by_reporter_stat, subdir = "details")
  rprt_writetable(hs6cpcmap_total_stat,       subdir = "details")

  rprt_fulltable(hs6cpcmap_total_stat)
  rprt_fulltable(hs6cpcmap_by_reporter_stat)

}

