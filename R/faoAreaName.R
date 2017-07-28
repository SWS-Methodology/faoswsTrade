#' @import dplyr
#' @export

faoAreaName <- function(areacode,
                    swsdomain = "faostat_one",
                    swsdataset = "FS1_SUA_UPD",
                    swsdimension = "geographicAreaFS") {


  if (!is.integer(areacode))
    areacode <- as.integer(areacode)

  # It is possible to ask SWS on specific codes, but if it is absent, you
  # get a 500 error from the server. So it is easier to get full set
  data.frame(areacode) %>%
    left_join(faosws::GetCodeList(swsdomain, swsdataset, swsdimension) %>%
                as.data.frame() %>%
                select_(~code, ~description) %>%
                mutate_(code = ~as.integer(code)),
              by = c("areacode" = "code")) %>%
    select_(~description) %>%
    unlist() %>%
    unname()

}
