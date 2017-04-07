#' Check for parallel support and register CPU cores
#'
#' @import futile.logger
#' @export
#' @return TRUE if CPU cores were registered

register_cpu_cores <- function() {

  if(all(c("doParallel", "foreach") %in%
         rownames(installed.packages(noCache = TRUE)))) {

    flog.debug("Multicore backend is available.")

    cpucores <- parallel::detectCores(all.tests = TRUE)

    flog.debug("CPU cores detected: %s.", cpucores)

    doParallel::registerDoParallel(cores = cpucores)

    multicore <- TRUE

  } else {
    flog.debug("Multicore backend is not available.")

    multicore <- FALSE
  }

  multicore
}
