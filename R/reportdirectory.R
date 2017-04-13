#' Create directory for writing a report and supplementary files
#'
#' @param user User name to include in the name.
#' @param year Year to include in the name.
#' @param create Should the directory be created. TRUE by default.
#' @param browsedir Should the directory be opened in system-default file
#'   browser. TRUE by default.
#'
#' @return String with full name of report directory.
#' @export
#'

reportdirectory <- function(sws_user, year, create = TRUE, browsedir = TRUE) {

  sws_share <- Sys.getenv("R_SWS_SHARE_PATH", unset = NA_character_)

  if(is.na(sws_share)) stop("System variable R_SWS_SHARE_PATH not set.")

  reportdir <- file.path(
    sws_share,
    sws_user,
    paste("complete_tf_cpc", year,
          format(Sys.time(), "%Y%m%d%H%M%S%Z"),
          sep = "_"))

  reportdir <- normalizePath(reportdir,
                             winslash='/',
                             mustWork = FALSE)
  stopifnot(!file.exists(reportdir))
  dir.create(reportdir, recursive = TRUE)

  # Open report directory in system default file browser
  if(browsedir & interactive()) browseURL(reportdir)

  reportdir

}