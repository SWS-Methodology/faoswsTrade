#' Create HS6->FCL mapping table.
#'
#' @import dplyr
#'
#' @param maptable hsfclmap data frame.
#' @return Data frame with columns reporter, flow, hs6, fcl
#' @export
#'

extract_hs6fclmap <- function(maptable = NULL, parallel = FALSE) {

  stopifnot(!is.null(maptable))

  if(!"reporter" %in% colnames(maptable) &
     "area" %in% colnames(maptable)) {
    maptable <- rename_(maptable, .dots = list(reporter = ~area))
  }

  maptable <- select_(maptable,
                      ~reporter,
                      ~flow,
                      ~fromcode,
                      ~tocode,
                      ~fcl,
                      ~recordnumb)

  maptable <- maptable %>%
    mutate_at(vars(ends_with("code")),
              funs(str_sub(., end = 6L))) %>%
    # Probably after trimming to hs6 we are getting duplicates
    distinct_(~reporter,
              ~flow,
              ~fromcode,
              ~tocode,
              ~fcl,
              .keep_all = TRUE) %>%
    # as.numeric in a separate step to run distinct on character
    # instead of numeric
    mutate_at(vars(ends_with("code")),
              as.numeric) %>%
    mutate_(hsrange = ~tocode - fromcode)

  maptable_0range <- maptable %>%
    filter_(~hsrange == 0) %>%
    select_(~reporter,
            ~flow,
            hs = ~fromcode,
            ~fcl,
            ~recordnumb)

  maptable_range <- maptable %>%
    filter_(~hsrange > 0) %>%
    plyr::ddply(.variables = c("reporter", "flow"),
                function(df) {
                  plyr::adply(df, 1L, function(df){
                    allhs <- seq.int(df$fromcode, df$tocode)
                    rows <- length(allhs)
                    fcl <- rep.int(df$fcl, times = rows)
                    recordnumb <- rep.int(df$recordnumb, times = rows)
                    data_frame(hs = allhs,
                               fcl = fcl,
                               recordnumb = recordnumb)
                  })
                },
                .parallel = parallel) %>%
    select_(~reporter,
            ~flow,
            ~hs,
            ~fcl,
            ~recordnumb)

  bind_rows(maptable_0range, maptable_range) %>%
    distinct() %>%
    # split by chunks to be efficient in parallel execution
    plyr::ddply(.variables = c("reporter", "flow"),
                function(df) {
                  df %>%
                    group_by_(~hs) %>%
                    mutate_(fcl_links = ~length(unique(fcl))) %>%
                    ungroup()
                },
                .parallel = parallel)
}
