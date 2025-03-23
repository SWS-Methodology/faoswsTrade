#' Save file for pre-processing report.
#'
#' @param data Data to save.
#' @param file File name.
#' @param type FIle type: can be either "csv" or "rds" and the data will
#'   be saved as a csv or rds file, respectively. If this parameter is
#'   missing, the file type will be read from the extension of the `file`
#'   parameter (and will be used only if it is "csv" or "rds").
#' @param sleep Amount of time to sleep between each one of the `times`
#'   attempted saves.
#' @param times How many times try to save the file.
#'
#' @return Nothing: used for side effect (save file).
#'
#' @export

saveFileForPPR <- function(data = NA, file = NA, type = NA,
                           sleep = 10, times = 10) {

  if (missing(data)) stop('"data" is missing')

  if (missing(file)) stop('"file" is missing')

  if (missing (type) || !(tolower(type) %in% c('csv', 'rds'))) {
    type <- tolower(sub('.*\\.(...)$', '\\1', file))
    if (!(type %in% c('csv', 'rds'))) {
      stop('"type" or the file extension should be either "csv" or "rds"')
    }
  }

  # just in case there is a dot
  type <- sub('^\\.', '', type)

  if (tolower(type) == 'rds') {
    a <- try(saveRDS(data, file = file))
  } else if (tolower(type) == 'csv') {
    a <- try(write.csv(data, file = file, row.names = FALSE))
  }

  i <- 1
  while (inherits(a, 'try-error') && i <= times) {
    msg <- paste('File writing failed. Attempt n.', times,
      'Sleeping for', sleep, 'seconds.')

    warning(msg, immediate. = TRUE)

    flush.console()

    Sys.sleep(sleep)

    i <- i+1

    if (tolower(type) == 'csv') {
      a <- try(saveRDS(data, file = file))
    } else {
      a <- try(write.csv(data, file = file, row.names = FALSE))
    }
  }

  if (inherits(a, 'try-error')) {
    stop(paste('Could not write', file))
  }
}


