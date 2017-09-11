#' Compute accuracy scores.
#'
#' @param data data.
#' @param type string: what kind of scores should be computed ("local", default,
#'   or "global"). If set to "global", the \code{igraph} library needs to be
#'   available.
#' @param method string: what method to use in order to compute scores
#'   ("correlation", default, or "cosine", "ejaccard", "edice",
#'    "bhjattacharyya", "canberra", "divergence", "wave", "kullback",
#'    "bray", "soergel", "chord", "geodesic", "whittaker", "hellinger").
#'   Except from "correlation", the other methods require the \code{proxy}
#'   library.
#'
#' @return Reshaped data.
#'
#' @import dplyr
#'
#' @export

# TODO: parameterise names of variables? (qty, qty_m could change in the future)
accuracyScores <- function(data = NA, type = 'local', method = 'correlation') {

  if (missing(data)) stop('"data" is required.')

  # Distances that could be used from package 'proxy':
  #   c('cosine', 'ejaccard', 'edice', 'bhjattacharyya',
  #     'canberra', 'divergence', 'wave', 'kullback', 'bray',
  #     'soergel', 'chord', 'geodesic', 'whittaker', 'hellinger')

  if (method == 'correlation') {
    # Not using simil() as it would take longer
    fun <- function(x, y, z) cor(x, y)
  } else {
    library(proxy)

    fun <- function(x, y, z) simil(x, y, method = z, by_rows = FALSE)[1,1]
  }

  tmp_accu <- data %>%
    filter(complete.cases(qty, qty_m, cpc)) %>%
    group_by(reporter, flow, partner) %>%
    summarise(correl = fun(qty, qty_m, method), n = n()) %>%
    group_by(reporter) %>%
    mutate(tot = sum(n, na.rm = TRUE), wt = n/tot) %>%
    ungroup() %>%
    filter(n > 1) %>%
    mutate(score = correl*wt)

  # These can be non-reporters or (less likely) countries with a single flow
  nonrep <- unique(data$reporter)[!(unique(data$reporter) %in% unique(tmp_accu$reporter))]

  # Giving the minimun score to coutries that never show as reporters
  tmp_accu <-
    bind_rows(
              tmp_accu,
              tmp_accu[rep(1, length(nonrep)),] %>%
                mutate(
                       reporter = nonrep,
                       correl   = NA,
                       n        = 0,
                       tot      = 0,
                       wt       = 0,
                       score    = min(tmp_accu$correl, na.rm = TRUE)
                       )
              )

  if (type == 'local') {
    accu <- tmp_accu %>%
              group_by(reporter) %>%
              summarise(accu_score = sum(score, na.rm = TRUE)) %>%
              ungroup() %>%
              rename(country = reporter) %>%
              mutate(
                     accu_rank  = rank(-accu_score),
                     accu_group = ntile(accu_rank, 10)
                     )
  } else {
    global_accu <- tmp_accu %>%
      group_by(reporter, partner) %>%
      summarise(score = sum(score, na.rm = TRUE)) %>%
      ungroup() %>%
      tidyr::spread(partner, score) %>%
      select(-reporter) %>%
      as.matrix()

    global_accu[is.na(global_accu)] <- 0

    dimnames(global_accu)[[1]] <- dimnames(global_accu)[[2]]

    g <- global_accu %>%
      igraph::graph_from_adjacency_matrix(mode = 'directed', weighted = TRUE)

    accu <- tibble(
                   country    = dimnames(global_accu)[[1]],
                   accu_score = igraph::page_rank(g)$vector
                   ) %>%
            mutate(
                   accu_rank  = rank(-accu_score),
                   accu_group = ntile(accu_rank, 10)
                   )
  }

  # Giving the minimun score to coutries that never show as reporters, and a new group
  if (length(nonrep) > 0) {
    accu <- bind_rows(accu, data_frame(country = nonrep, accu_score = 0, accu_rank = nrow(accu), accu_group = 11))
  }

  return(accu)
}
