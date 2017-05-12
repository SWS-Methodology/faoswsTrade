#' Check package versions requirements
#'
#' @param packages Character vector of packages' names to check
#' @param versions Character vectorr of minimal packages' versions
#'
#' @export
#'
check_versions <- function(packages, versions) {

    min_versions <- data.frame(package = packages,
                               version = versions,
                               stringsAsFactors = FALSE)

    for (i in nrow(min_versions)){
      # installed version
      p <- packageVersion(min_versions[i,"package"])
      # required version
      v <- package_version(min_versions[i,"version"])
      if(p < v){
        stop(sprintf("%s >= %s required", min_versions[i,"package"], v))
      }
    }

    invisible(min_versions)
}
