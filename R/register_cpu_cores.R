#' Check for parallel support and register CPU cores
#'
#' @import futile.logger
#' @export
#' @return TRUE if CPU cores were registered

register_cpu_cores <- function() {

  if(all(c("doParallel", "foreach") %in%
         rownames(installed.packages(noCache = TRUE)))) {

    flog.debug("Packages doParallel and foreach are available.", name = "dev")

    if (CheckDebug()) {
      cpucores <- parallel::detectCores(all.tests = TRUE) - 1
    } else {
      # When running on SWS it is better to limit the
      # maximum number of cores used.
      cpucores <- 3
    }

    flog.debug("CPU cores detected: %s.", cpucores, name = "dev")

    doParallel::registerDoParallel(cores = cpucores)

    multicore <- TRUE

    flog.debug("Parallel backend registered and will be used", name = "dev")

  } else {
    flog.debug("Packages doParallel and foreach are not available", name = "dev")
    flog.debug("Parallel execution is disabled",  name = "dev")

    multicore <- FALSE
  }

  multicore
}
