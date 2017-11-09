#' Update the table with information of module runs.
#'
#' @param year The year.
#' @param mode String: which type of update will be done. "restart" means
#'   only the "restarted" date string will be updated (it will also update
#'   the "info" variable by setting it to "restarted"; "save" will update the
#'   table with information to the data inserted/appended/ignored/discarded,
#'   and will update the "saved" variable (it will also update the "info"
#'   variable by inserting the hours required by the module).
#' @param results The object returned by faosws::SaveData().
#'
#' @return NULL.
#'
#' @import faosws
#' @import data.table
#'
#' @export

updateInfoTable <- function(year = NA, mode = NA, results = NA) {

  if (missing(year)) {
    stop('"year" is missing.')
  }

  if (missing(mode) || !(mode %in% c('restart', 'save'))) {
    stop('"mode" should be either "restart" or "save".')
  }

  if (missing(results) && mode == 'save') {
    stop('"results", i.e., the object returned by SaveData(), is missing.')
  }

  changeset <- Changeset('complete_tf_runs_info')

  existing_tab <- ReadDatatable('complete_tf_runs_info', readOnly = FALSE)

  if (nrow(existing_tab) == 0) {
    warning('The info table is empty. Should it be like that?')
  }

  # if this is TRUE then a new year needs to be appended
  # (i.e., that year was never done before)
  add_year <- ifelse(year %in% existing_tab$year, FALSE, TRUE)

  ######################################################################
  ## Need to delete on server, the updated table will be loaded after ##
  ######################################################################

  AddDeletions(changeset, existing_tab)

  Finalise(changeset)

  existing_tab <- existing_tab[, c('year',
                                   'inserted',
                                   'appended',
                                   'ignored',
                                   'discarded',
                                   'saved',
                                   'restarted',
                                   'info')]


  if (add_year) {
    new_year_row <-
      data.table(
        year      = year,
        inserted  = 0,
        appended  = 0,
        ignored   = 0,
        discarded = 0,
        saved     = NA_character_,
        restarted = NA_character_,
        info      = NA_character_
      )

    existing_tab <- rbind(existing_tab, new_year_row)
  }

  existing_tab <- existing_tab[order(existing_tab$year),]

  cols <- c('year', 'inserted', 'appended', 'ignored', 'discarded')

  if (mode == 'restart') {

    ###########################################################
    ##### Update "restarted" when module is started again #####
    ###########################################################

    if (!exists('startTime')) {
      # It should have been set at the beginning. If it is not,
      # we create that now (it will lag just by some seconds)
      startTime <- Sys.time()
    }

    restarted_time <- format(startTime, '%Y-%m-%d %H:%M')

    existing_tab$restarted[existing_tab$year == year] <- restarted_time

    existing_tab$info[existing_tab$year == year] <- 'restarted'

    AddInsertions(changeset, existing_tab)
  }

  if (mode == 'save') {

    stopifnot(cols[-1] %in% names(results))

    ####################################################
    ##### Update info when module ran successfully #####
    ####################################################

    idx <- existing_tab$year != year

    info_table_no_last <- existing_tab[idx, ]

    new_infotable_row <-
      data.table(
        year      = year,
        inserted  = results$inserted,
        appended  = results$appended,
        ignored   = results$ignored,
        discarded = results$discarded,
        saved     = format(Sys.time(), '%Y-%m-%d %H:%M'),
        restarted = existing_tab[!idx, ]$restarted,
        info      = NA_character_
      )


    new_infotable_row[, (cols) := lapply(.SD, as.integer), .SDcols = cols]

    hours_passed <- round(as.numeric(difftime(
                                       new_infotable_row$saved,
                                       new_infotable_row$restarted,
                                       units = 'hours')), 2)

    new_infotable_row$info <- paste(hours_passed, 'hours')

    new_infotable <- rbind(info_table_no_last, new_infotable_row)

    new_infotable <- new_infotable[order(new_infotable$year), ]

    AddInsertions(changeset, new_infotable)
  }

  Finalise(changeset)

  return(NULL)
}

