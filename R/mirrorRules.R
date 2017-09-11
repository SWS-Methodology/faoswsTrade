#' Use mirror rules for balancing trade.
#'
#' @param data data.
#'
#' @return Reshaped data with mirror flows, if necessary.
#'
#' @import dplyr
#'
#' @export

mirrorRules <- function(data          = NA,
                         variable      = 'qty',
                         suffix        = '_m',
                         flag          = 'flag_qty',
                         group_rank    = 'accu_group',
                         flow          = 'flow',
                         main_flow     = 1,
                         discrepancy   = 'discrep_mirr',
                         # FIXME there are some '-' and some '-h': why?
                         official_flags = c('X-', '-', '-h')) {

  variable_mirr <- paste0(variable, suffix)
  flag_mirr <- paste0(flag, suffix)
  group_rank_mirr <- paste0(group_rank, suffix)

 # We need to check that qty exists otherwise it's a
 # mirror flow that originally was the opposite flow
 best_flow <- ifelse(data[[flow]] == main_flow & !is.na(data[[variable]]), 'rep', 'prt')
 # Default on mirror, though it couldn't be official (reporter data is NA)
 # XXX Have to put here the "official" flag (in this case is "-", given that obs is
 # "<BLANK>" and method is "<BLANK>")
 official <- if_else(data[[flag]] %in% official_flags, 'rep', 'prt', 'prt')


  res <- case_when(
    # If only mirror exists use it
    is.na(data[[variable]]) & !is.na(data[[variable_mirr]])     ~ 'prt',
    # If there NO discrepancy, leave reporter data untouched
    !(data[[discrepancy]] %in% TRUE) & !is.na(data[[variable]]) ~ 'rep',
    # In the remaining cases there should be a discrepancy.
    # If accuracy group is lower then accuracy score is higher
    data[[group_rank]] < data[[group_rank_mirr]]                ~ 'rep',
    # XXX how come the following number is equal to the previous one?
    data[[group_rank_mirr]] < data[[group_rank]]                ~ 'prt',
    # If same flags, use "main_flow" if it exists
    ((data[[flag]] %in% official_flags) ==
     (data[[flag_mirr]] %in% official_flags)) %in% TRUE         ~ best_flow,
    # Below flags should be different, thus use official data
    TRUE                                                        ~ official 
    )

  var_res <- ifelse(res == 'rep', data[[variable]], data[[variable_mirr]])

  var_flag <- ifelse(res == 'rep', data[[flag]], data[[flag_mirr]])

  df <- data.frame(variable = var_res, flag = var_flag, stringsAsFactors = FALSE) 

  return(df)
}

