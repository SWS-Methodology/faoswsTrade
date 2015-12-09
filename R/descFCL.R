#' @import dplyr
#' @export

descFCL <- function(item,
                    swsdomain = "trade",
                    swsdataset = "completed_tf_fcl",
                    swsdimension = "measuredItemFS") {


  if (!is.integer(item))
    item <- as.integer(item)

  if(is.SWSEnvir()) {
    fclcodelist <- faosws::GetCodeList(swsdomain,
                                       swsdataset,
                                       swsdimension)
  } else data("fclcodelist", package = "tradeproc", envir = environment())

  # It is possible to ask SWS on specific codes, but if it is absent, you
  # get a 500 error from the server. So it is easier to get full set
  data.frame(item) %>%
    left_join(fclcodelist %>%
                as.data.frame() %>%
                select_(~code, ~description) %>%
                mutate_(code = ~as.integer(code)),
              by = c("item" = "code")) %>%
    select_(~description) %>%
    unlist() %>%
    unname()

}
