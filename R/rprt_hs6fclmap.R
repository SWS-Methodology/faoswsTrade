#' Reports on results of extraction of HS6->FCL map
#'
#' @param hs6fclmap produced by extract_hs6fclmap function
#' @import dplyr

rprt_hs6fclmap <- function(hs6fclmap = NULL) {

  stopifnot(!is.null(hs6fclmap))

  hs6fclmap_by_reporter_stat <- hs6fclmap %>%
    group_by_(~reporter, ~flow) %>%
    mutate(total_links = n()) %>%
    group_by_(~total_links, ~hs6, add = TRUE) %>%
    summarise(one2one = n() == 1L) %>%
    group_by_(~reporter, ~flow, ~total_links) %>%
    summarise_(
      uniq_hs6 = ~n(),
      one2one_count = ~sum(one2one),
      one2one_prop = ~one2one_count / uniq_hs6) %>%
    ungroup()

  hs6fclmap_total_stat <- hs6fclmap_by_reporter_stat %>%
    summarise_at(vars(total_links, uniq_hs6, one2one_count), funs(sum)) %>%
    mutate_(one2one_prop = ~one2one_count / uniq_hs6)

  hs6fclmap_by_reporter_stat <- add_area_names(hs6fclmap_by_reporter_stat,
                                               "fao")
  hs6fclmap <- add_area_names(hs6fclmap, "fao")

  rprt_writetable(hs6fclmap)
  rprt_writetable(hs6fclmap_by_reporter_stat)
  rprt_writetable(hs6fclmap_total_stat)
  rprt_fulltable(hs6fclmap_total_stat, pretty_prop = TRUE)
  rprt_fulltable(hs6fclmap_by_reporter_stat, pretty_prop = TRUE)

}
