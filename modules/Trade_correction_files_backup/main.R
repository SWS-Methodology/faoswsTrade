##'
##' **Author: Aydan Selek**
##'
##' **Description:**
##'
##' This module is designed to collect all the correction files created by the Shiny application and send them by email to the target people.
##' It is specific use is to have a backup of these files from the cloud server.
##'
##'
##' **Inputs:**
##'
##' * correction files created by the Shiny application
##'
##' **Flag assignment:**
##'
##' None


## Load the libraries
library(faosws)
library(data.table)
library(faoswsUtil)
library(sendmailR)
library(openxlsx)
library(dplyr)

# E-mail addresses of people that will get notified.
EMAIL_RECIPIENTS <- ReadDatatable("ess_trade_people")$fao_email
EMAIL_RECIPIENTS <- gsub(" ", "", EMAIL_RECIPIENTS)

# Remove S.T.:
EMAIL_RECIPIENTS <- EMAIL_RECIPIENTS[!grepl("yy", EMAIL_RECIPIENTS)]

sendMailAttachment = function(fileToSend,name,textBody) {

  if(dim(fileToSend)[1]>0){

    if(!CheckDebug()){
      # Create the body of the message
      FILETYPE = ".csv"
      CONFIG <- faosws::GetDatasetConfig(swsContext.datasets[[1]]@domain, swsContext.datasets[[1]]@dataset)
      sessionid <- ifelse(length(swsContext.datasets[[1]]@sessionId),
                          swsContext.datasets[[1]]@sessionId,
                          "core")

      basename <- sprintf("%s_%s",
                          name,
                          sessionid)

      basedir <- tempfile()
      dir.create(basedir, recursive = TRUE)
      destfile <- file.path(basedir, paste0(basename, FILETYPE))

      # create the csv in a temporary foldes
      write.csv(fileToSend, destfile, row.names = FALSE)
      # define on exit strategy
      on.exit(file.remove(destfile))
      body = textBody

      sendmailR::sendmail(from = "sws@fao.org",
                          to = EMAIL_RECIPIENTS,
                          subject = name,
                          msg = list(strsplit(body,"\n")[[1]],
                                     sendmailR::mime_part(destfile,
                                                          name = paste0(basename, FILETYPE)
                                     )
                          )
      )
    }
  }
}

R_SWS_SHARE_PATH <- Sys.getenv("R_SWS_SHARE_PATH")

if (CheckDebug()) {
  library(faoswsModules)
  SETTINGS = ReadSettings("modules/Trade_correction_files_backup/sws.yml")
  ## Define where your certificates are stored
  faosws::SetClientFiles(SETTINGS[["certdir"]])
  ## Get session information from SWS. Token must be obtained from web interface
  GetTestEnvironment(baseUrl = SETTINGS[["server"]],
                     token = SETTINGS[["token"]])
}

`%!in%` = Negate(`%in%`)

corrections_dir <-
  file.path(Sys.getenv('R_SWS_SHARE_PATH'), 'trade/validation_tool_files')

# Corrections are stored into single-country files
corrections_table_all <-
  lapply(
    file.path(dir(corrections_dir, pattern = '^[0-9]+$', full.names = TRUE),
              'corrections_table.rds'),
    readRDS
  ) %>%
  bind_rows()


bodyCorrections <- paste0("Please find the attached file with the corrections saved by Shiny tool.
                          This file has been sent to you to keep back-up of the corrections. Therefore, it is enough to keep the latest email.")

sendMailAttachment(corrections_table_all, "The Shiny corrections back-up", bodyCorrections)
message("Shiny correction back-up file has been sent")
