##'
##'
##' **Author: Amsata Niang**
##'
##' **Description:**
##'
##' This module is designed to synchronize production data from SUA balance to agriv=culture domain
##'

message("plug-in starts to run")

# Import libraries
suppressMessages({
  library(data.table)
  library(faosws)
  library(faoswsFlag)
  library(faoswsUtil)
  library(faoswsImputation)
  library(faoswsProduction)
  library(faoswsProcessing)
  library(faoswsEnsure)
  library(magrittr)
  library(dplyr)
  library(ggplot2)
  library(sendmailR)
  library(faoswsStandardization)
})

options(scipen = 999)
R_SWS_SHARE_PATH = Sys.getenv("R_SWS_SHARE_PATH")

if(CheckDebug()){

    library(faoswsModules)
    SETTINGS = ReadSettings("modules\\SUA_Trade_Synchronization\\sws.yml")

    ## If you're not on the system, your settings will overwrite any others
    R_SWS_SHARE_PATH = SETTINGS[["share"]]

    ## Define where your certificates are stored
    SetClientFiles(SETTINGS[["certdir"]])

    ## Get session information from SWS. Token must be obtained from web interface
    GetTestEnvironment(baseUrl = SETTINGS[["server"]],
                       token = SETTINGS[["token"]])

}

startYear = as.numeric(swsContext.computationParams$start_year)
endYear = as.numeric(swsContext.computationParams$end_year)
commodity = as.character(swsContext.computationParams$element)


#geoM49 = swsContext.computationParams$geom49
stopifnot(startYear <= endYear)
yearVals = startYear:endYear
USER <- regmatches(
  swsContext.username,
  regexpr("(?<=/).+$", swsContext.username, perl = TRUE)
)
`%!in%`<-Negate(`%in%`)

send_mail <- function(from = NA, to = NA, subject = NA,
                      body = NA, remove = FALSE) {

  if (missing(from)) from <- 'no-reply@fao.org'

  if (missing(to)) {
    if (exists('swsContext.userEmail')) {
      to <- swsContext.userEmail
    }
  }

  if (is.null(to)) {
    stop('No valid email in `to` parameter.')
  }

  if (missing(subject)) stop('Missing `subject`.')

  if (missing(body)) stop('Missing `body`.')

  if (length(body) > 1) {
    body <-
      sapply(
        body,
        function(x) {
          if (file.exists(x)) {
            # https://en.wikipedia.org/wiki/Media_type
            file_type <-
              switch(
                tolower(sub('.*\\.([^.]+)$', '\\1', basename(x))),
                txt  = 'text/plain',
                csv  = 'text/csv',
                png  = 'image/png',
                jpeg = 'image/jpeg',
                jpg  = 'image/jpeg',
                gif  = 'image/gif',
                xls  = 'application/vnd.ms-excel',
                xlsx = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                doc  = 'application/msword',
                docx = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
                pdf  = 'application/pdf',
                zip  = 'application/zip',
                # https://stackoverflow.com/questions/24725593/mime-type-for-serialized-r-objects
                rds  = 'application/octet-stream'
              )

            if (is.null(file_type)) {
              stop(paste(tolower(sub('.*\\.([^.]+)$', '\\1', basename(x))),
                         'is not a supported file type.'))
            } else {
              res <- sendmailR:::.file_attachment(x, basename(x), type = file_type)

              if (remove == TRUE) {
                unlink(x)
              }

              return(res)
            }
          } else {
            return(x)
          }
        }
      )
  } else if (!is.character(body)) {
    stop('`body` should be either a string or a list.')
  }

  sendmailR::sendmail(from, to, subject, as.list(body))
}



TMP_DIR <- file.path(tempdir(), USER)
if (!file.exists(TMP_DIR)) dir.create(TMP_DIR, recursive = TRUE)

tmp_file_synch<- file.path(TMP_DIR, paste0("Synchronize_SUA_APR_",commodity,".csv"))
tmp_file_country<- file.path(TMP_DIR, paste0("Country_Stats_",commodity,".csv"))
tmp_file_crostab<- file.path(TMP_DIR, paste0("Flag_comb_Freq_",commodity,".csv"))
tmp_file_Ec_flag<- file.path(TMP_DIR, paste0("Freq_Ec_country_",commodity,".csv"))



Utilization_Table <- ReadDatatable("utilization_table_2018")
# Get production data from SUA domain

sessionKey = swsContext.datasets[[2]]
datasetConfig = GetDatasetConfig(domainCode = sessionKey@domain,
                                 datasetCode = sessionKey@dataset)

prod_sua <- GetData(sessionKey)


if (commodity=="imports") {
  items=intersect(Utilization_Table[primary_item == 'X', cpc_code],
                  unique(prod_sua[,get("measuredItemFbsSua")]))
  eleKeys="5610"
} else if (commodity=="exports"){

  items=intersect(Utilization_Table[derived == 'X', cpc_code],
  unique(prod_sua[,get("measuredItemFbsSua")]))
  eleKeys="5910"
}


prod_sua<-prod_sua[measuredItemFbsSua %in% items & measuredElementSuaFbs %in% eleKeys,]
prod_sua<-rbind(
  prod_sua[measuredElementSuaFbs=="5510" & flagObservationStatus %in% c("","E","T"),],
  prod_sua[measuredElementSuaFbs %in% setdiff(eleKeys,"5510"),]
)
prod_sua<-rbind(
  prod_sua[ measuredElementSuaFbs=="5510" & flagMethod %!in% c("i","t","e"),],
  prod_sua[measuredElementSuaFbs %in% setdiff(eleKeys,"5510"),]
)

prod_sua<-prod_sua[!(flagObservationStatus=="" & flagMethod==""),]
prod_sua<-prod_sua[!(flagObservationStatus=="M" & flagMethod=="-"),]


# Get production data from production domain

selectedGEOCode =
    getQueryKey("geographicAreaM49", sessionKey)

geoKeys = GetCodeList(domain = "agriculture", dataset = "aproduction",
                      dimension = "geographicAreaM49")[type == "country", code]

message("Pulling data from Agriculture Production")

## if the

geoDim = Dimension(name = "geographicAreaM49", keys = selectedGEOCode)


# eleKeys = c("5510","5520","5525","5016")

## Combine with single codes
eleDim = Dimension(name = "measuredElementTrade", keys =eleKeys)

itemKeys=items

itemDim = Dimension(name = "measuredItemCPC", keys = itemKeys)
timeDim = Dimension(name = "timePointYears", keys = as.character(yearVals))
agKey = DatasetKey(domain = "trade", dataset = "total_trade_cpc_m49",
                   dimensions = list(
                       geographicAreaM49 = geoDim,
                       measuredElementTrade = eleDim,
                       measuredItemCPC = itemDim,
                       timePointYears = timeDim)
)


prod_apr <- GetData(agKey)


#Harmonize the colunm name
names(prod_sua)<-names(prod_apr)

#Select production data for the selected year range
prod_apr<-prod_apr[timePointYears %in% yearVals & measuredElementTrade %in% eleKeys,]
prod_sua<-prod_sua[timePointYears %in% yearVals & measuredElementTrade %in% eleKeys,]





prod_apr[,Flag_apr:=paste0("[",flagObservationStatus,",",flagMethod,"]")]
prod_sua[,Flag_sua:=paste0("[",flagObservationStatus,",",flagMethod,"]")]



synch_data<-merge(
    prod_apr[,list(geographicAreaM49,
                   measuredElementTrade,
                   measuredItemCPC,
                   timePointYears,Value,
                   Flag_apr)],
    prod_sua[,list(geographicAreaM49,
                   measuredElementTrade,
                   measuredItemCPC,
                   timePointYears,
                   prod_sua=Value,
                   flagObservationStatus,
                   flagMethod,
                   Flag_sua)],
    by=c("geographicAreaM49",
         "measuredElementTrade",
         "measuredItemCPC",
         "timePointYears"),
    all.y = TRUE # keep all SUA data even missing in production env
)

synch_data[,check:=dplyr::near(round(Value),round(prod_sua))]
synch_data[is.na(check),check:=F]
data_analyse<-
  synch_data[measuredElementTrade %in% eleKeys & check==FALSE,]



data_analyse=data_analyse[,`:=`(flagObservationStatus=NULL,flagMethod=NULL,check=NULL)]
setnames(data_analyse,"Value","prod_apr")

data_analyse <-
  nameData(
    "agriculture",
    "aproduction",
    data_analyse
  )

data_analyse=data_analyse[,`(SUA-APR)/SUA %`:=round(1-prod_apr/prod_sua,2)*100][order(-`(SUA-APR)/SUA %`),]
data_analyse[,`SUA-APR`:=prod_sua-prod_apr]

data_stat_by_country=data_analyse[,list(frequency=.N),
               by=c("geographicAreaM49")]
write.csv(data_analyse, tmp_file_synch)
write.csv(data_stat_by_country, tmp_file_country)

data_plot_flag_apr=data_analyse[,list(frequency=.N),
               by=c("Flag_apr")]
data_plot_flag_sua=data_analyse[,list(frequency=.N),
                by=c("Flag_sua")]

data_plot_flag_main=data_analyse[,list(frequency=.N),
                    by=c("Flag_sua","Flag_apr")]

write.csv(data_plot_flag_main, tmp_file_crostab)


data_flag_Ec=data_analyse[,list(frequency=.N),
                                 by=c("geographicAreaM49","Flag_sua")][
                                    Flag_sua=="[E,c]",]

data_flag_Ec<-nameData("agriculture",
                       "aproduction",
                       data_flag_Ec)

total<-data_flag_Ec[1]
total$geographicAreaM49[1]=999
total$geographicAreaM49_description[1]="All COuntries"
total$Flag_sua="[E,c]"
total$frequency=sum(data_flag_Ec$frequency,na.rm = TRUE)

data_flag_Ec<-rbind(total,data_flag_Ec)

write.csv(data_flag_Ec, tmp_file_Ec_flag)


plot_apr<-ggplot(data_plot_flag_apr,aes(x=Flag_apr,y=frequency,fill=Flag_apr))+geom_col()+theme_classic()+
  theme(legend.position = "none")+
  labs(title="Flag typology of removed production data points",
       subtitle=paste0("Year range=",startYear,"-",endYear),
       y="Number of removed data point")+
  geom_text(data=data_plot_flag_apr, aes(x = Flag_apr,y=frequency+25,
                         label=paste0(frequency,"\n(",round(frequency/sum(frequency)*100,1),"%)")),
            size=2.5)
tmp_file_plot_apr <-file.path(TMP_DIR, paste0("PLOT_FLAG_APR", ".pdf"))
ggsave(tmp_file_plot_apr, plot = plot_apr)


plot_sua<-ggplot(data_plot_flag_sua,aes(x=Flag_sua,y=frequency,fill=Flag_sua))+geom_col()+theme_classic()+
  theme(legend.position = "none")+
  labs(title="Flag typology of synchronized sua data points",
       subtitle=paste0("Year range=",startYear,"-",endYear),
       y="Number of removed data point")+
  geom_text(data=data_plot_flag_sua, aes(x = Flag_sua,y=frequency+35,
                         label=paste0(frequency,"\n(",round(frequency/sum(frequency)*100,1),"%)")),
            size=2.5)
tmp_file_plot_sua<-file.path(TMP_DIR, paste0("PLOT_SUA_FLAG", ".pdf"))
ggsave(tmp_file_plot_sua, plot = plot_sua)


plot_main<-ggplot(data_plot_flag_main,aes(Flag_apr,frequency,fill=Flag_sua))+geom_col()+
  theme_classic()+
  labs(title="Flag typology of removed production data points",
       subtitle=paste0("Year range=",startYear,"-",endYear),
       y="Number of removed data point")

tmp_file_plot_main <-file.path(TMP_DIR, paste0("PLOT_MAIN",".pdf"))
ggsave(tmp_file_plot_main, plot = plot_main)

to_save_to_apr<-rbind(
  synch_data[measuredElementTrade=="5510" & check==FALSE,][,list(geographicAreaM49,
                                                                 measuredElementTrade,
                               measuredItemCPC,
                               timePointYears,
                               Value=prod_sua,
                               flagObservationStatus,
                               flagMethod)],
  synch_data[measuredElementTrade %in% setdiff(eleKeys,"5510"),][,list(geographicAreaM49,
                                                                       measuredElementTrade,
                                                            measuredItemCPC,
                                                            timePointYears,
                                                     Value=prod_sua,
                                                            flagObservationStatus,
                                                            flagMethod)]

)



sessionKey = swsContext.datasets[[1]]
datasetConfig = GetDatasetConfig(domainCode = sessionKey@domain,
                                 datasetCode = sessionKey@dataset)


SaveData(domain = datasetConfig$domain,
         dataset = datasetConfig$dataset,
         data = to_save_to_apr, waitTimeout = 2000000)



body_message <-
  sprintf(
    "################################################
      # SUA/APR SYNCHRONIZATION #
      ################################################

      ###############################################
      ##############       DONE!       ##############
      ###############################################

      "
  )

if (!CheckDebug()) {
  send_mail(
    from = "do-not-reply@fao.org",
    to = swsContext.userEmail,
    subject = "Synchronization completed!",
    body = c(body_message,
             tmp_file_synch,
             tmp_file_country,
             tmp_file_crostab,
             tmp_file_Ec_flag,
             tmp_file_plot_sua,
             tmp_file_plot_apr,
             tmp_file_plot_main
    )
  )
}

unlink(TMP_DIR, recursive = TRUE)

