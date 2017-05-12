#' Set SWS environment in development mode
#'
#' Massive side effects in R session and system environment.
#'
#' @param localsettingspath Path to a file with local SWS settings. See
#'   faoswsModules::ReadSettings for details.
#' @import dplyr
#' @import futile.logger
#' @export
#' @return invisible settings.
#'

set_sws_dev_settings <- function(localsettingspath = NULL) {

  stopifnot(!is.null(localsettingspath))
  SETTINGS <- faoswsModules::ReadSettings(localsettingspath)
  flog.debug("Local settings read from %s",
             localsettingspath, name = "dev")
  flog.debug("Local settings read:",
             SETTINGS,
             capture = TRUE, name = "dev")

  USER <<- if_else(.Platform$OS.type == "unix",
                   Sys.getenv('USER'),
                   Sys.getenv('USERNAME'))

  ## Define where your certificates are stored
  faosws::SetClientFiles(SETTINGS[["certdir"]])

  ## Get session information from SWS. Token must be obtained from web interface
  faosws::GetTestEnvironment(baseUrl = SETTINGS[["server"]],
                             token = SETTINGS[["token"]])

  # R_SWS_SHARE_PATH: 1) environment; 2) user; 3) fallback
  if (is.na(Sys.getenv("R_SWS_SHARE_PATH", unset = NA))) {
    flog.debug("R_SWS_SHARE_PATH environment variable not found.", name = "dev")

    if (!is.na(SETTINGS[['share']]) & dir.exists(SETTINGS[['share']])) {
      flog.debug("A valid 'share' variable was found in %s",
                 localsettingspath, name = "dev")
      Sys.setenv("R_SWS_SHARE_PATH" = SETTINGS[['share']])
      flog.debug("R_SWS_SHARE_PATH set to %s",
                 SETTINGS[['share']], name = "dev")
    }
    else {
      # Fall-back R_SWS_SHARE_PATH var
      flog.debug("An invalid/inexistent 'share' variable in %s",
                 localsettingspath, name = "dev")
      Sys.setenv("R_SWS_SHARE_PATH" = tempdir())
      flog.debug("R_SWS_SHARE_PATH now points to R temp directory %s",
                 tempdir(), name = "dev")
    }
  }
  invisible(SETTINGS)
}
