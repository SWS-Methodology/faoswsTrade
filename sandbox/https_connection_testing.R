baseUrl <- "https://hqlqasws1.hq.un.fao.org:8181/sws"

baseUrl <- "https://hqlqasws1.hq.un.fao.org:9453/sws"
baseUrl <- "https://168.202.39.56:8444/sws"



baseUrl <- "https://168.202.39.56:8081/sws"

baseUrl <- "https://168.202.39.56:9090/sws"

token <- "da889579-5684-4593-aa36-2d86af5d7138"

url <- paste0(baseUrl, "/rest/r/computationParameters/",
              token)


response <- RCurl::getURL(
  url = url,
  curl = RCurl::getCurlHandle(),
  verbose = FALSE,
  noproxy = "*",
  ssl.verifypeer = FALSE,
  sslcert = file.path(Sys.getenv("HOME"), ".R", "client.crt"),
  sslkey = file.path(Sys.getenv("HOME"), ".R", "client.key"),
  ssl.verifyhost = 2,
  httpheader = c(Accept = "application/json",
                 `Content-Type` = "application/json"))
