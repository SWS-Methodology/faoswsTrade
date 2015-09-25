
reporters <- c(231, 238) # The USA and Ethiopia

.ojdbcclasspath <- file.path(Sys.getenv("HOME"), "dstrbs", "ojdbc14.jar")

valid <- fclhs::gettfvalid(reporter = reporters, year = 2011)

tldata <- tldata %>%
  mutate(qunit = as.integer(qunit))
