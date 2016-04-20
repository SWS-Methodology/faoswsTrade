##' This module is used to fill in the imputed values from the buildModel.R 
##' script.
##' 
##' Note: the two objects used to fill in the imputations are the modelYield and
##' modelProduction objects.  These objects, however, can also contain other
##' observations (i.e. the modelYield object could contain production
##' observations).  This is because the "modelYield" label is intended to mean
##' that these imputations were generated during the yield imputation process,
##' and thus production or area harvested values could also appear as the result
##' of balancing.
##' 

library(faosws)
library(faoswsUtil)
library(data.table)

## Setting up variables
areaVar = "geographicAreaM49"
yearVar = "timePointYears"
itemVar = "measuredItemCPC"
elementVar = "measuredElement"

defaultStartYear = 2010
defaultEndYear = 2014
yearsModeled = 20
minObsForEst = 5

## set up for the test environment and parameters
R_SWS_SHARE_PATH = Sys.getenv("R_SWS_SHARE_PATH")
DEBUG_MODE = Sys.getenv("R_DEBUG_MODE")

if(!exists("DEBUG_MODE") || DEBUG_MODE == ""){
    cat("Not on server, so setting up environment...\n")
    
    ## Define directories
    if(Sys.info()[7] == "josh"){
        apiDirectory = "~/Documents/Github/faoswsProduction/R/"
        R_SWS_SHARE_PATH = "/media/hqlprsws2_prod/"
        ## R_SWS_SHARE_PATH = "/media/hqlprsws2_prod"
    } else if(Sys.info()[7] == "rockc_000"){
        apiDirectory = "~/Github/faoswsProduction/R/"
        stop("Can't connect to share drives!")
    }

    ## Get SWS Parameters
    SetClientFiles(dir = "~/R certificate files/Production/")
    GetTestEnvironment(
        baseUrl = "https://hqlprswsas1.hq.un.fao.org:8181/sws",
        token = "e518d5c0-7316-4f21-9f6f-4d2aa666c0c2"
        # baseUrl = "https://hqlqasws1.hq.un.fao.org:8181/sws",
        # token = "b55f4ae3-5a0c-4514-b89e-d040112bf25e"
    )

    ## Source local scripts for this local test
    for(file in dir(apiDirectory, full.names = T))
        source(file)
}

if(is.null(swsContext.computationParams$startYear)){
    startYear = defaultStartYear
} else {
    startYear = as.numeric(swsContext.computationParams$startYear)
}
if(is.null(swsContext.computationParams$endYear)){
    endYear = defaultEndYear
} else {
    endYear = as.numeric(swsContext.computationParams$endYear)
}
countryM49 = swsContext.datasets[[1]]@dimensions$geographicAreaM49@keys

stopifnot(startYear <= endYear)

successCount = 0

## Loop through the items and save production/yield data:
for(singleItem in swsContext.datasets[[1]]@dimensions$measuredItemCPC@keys){
    
    formulaTuples = try(getYieldFormula(singleItem))
    if(is(formulaTuples, "try-error")){
        failureCount = failureCount + 1
        next
    }
        
    for(i in 1:nrow(formulaTuples)){
        ## Load the imputations from the two models
        loadDataRm = try({
            load(paste0(R_SWS_SHARE_PATH, "/browningj/production/prodModel_",
                        singleItem, "_", i, "_est_removed.RData"))
        })
        if(is(loadDataRm, "try-error")){
            rmModelYield = NULL
            rmModelProduction = NULL
        } else {
            rmModelYield = modelYield
            rmModelProduction = modelProduction
        }
        loadDataKp = try({
            load(paste0(R_SWS_SHARE_PATH, "/browningj/production/prodModel_",
                        singleItem, "_", i, "_est_kept.RData"))
        })
        if(is(loadDataKp, "try-error")){
            kpModelYield = NULL
            kpModelProduction = NULL
        } else {
            kpModelYield = modelYield
            kpModelProduction = modelProduction
        }
        
        ## We have two models: using ("kp" for keep") and not using ("rm" for 
        ## removed) estimates in model estimation.  We will use the first case 
        ## only when we don't have much available official/semi-official data. 
        ## We can use different models for production vs. yield, as data 
        ## availability may differ between the two cases.
        ## 
        ## Exclude cases with flagMethod == "i", as these aren't imputations of
        ## yield but rather production/area harvested imputed via balancing.
        rmModelYield$fit[flagMethod != "i", imputeCnt := .N, geographicAreaM49]
        kpCountries = rmModelYield$fit[imputeCnt > yearsModeled - minObsForEst,
                                       unique(geographicAreaM49)]
        rmModelYield$fit[, imputeCnt := NULL]
        modelYield = rbind(
            rmModelYield$fit[!geographicAreaM49 %in% kpCountries, ],
            kpModelYield$fit[geographicAreaM49 %in% kpCountries, ])
        ## Select the production observations as well:
        rmModelProduction$fit[flagMethod != "i", imputeCnt := .N,
                              geographicAreaM49]
        kpCountries = rmModelProduction$fit[imputeCnt > yearsModeled - minObsForEst,
                                            unique(geographicAreaM49)]
        rmModelProduction$fit[, imputeCnt := NULL]
        modelProduction = rbind(
            rmModelProduction$fit[!geographicAreaM49 %in% kpCountries, ],
            kpModelProduction$fit[geographicAreaM49 %in% kpCountries, ])
        
        ## Verify that the years requested by the user (in swsContext.params) are
        ## possible based on the constructed model.
        obsStartYear = min(modelProduction$timePointYears)
        obsEndYear = max(modelProduction$timePointYears)
        if(any(!startYear:endYear %in% years)){
            stop(paste0("Model has been constructed on data that does not ",
                        "contain the desired imputation range!  Please ",
                        "update start and end year to fall in this range: ",
                        paste(years, collapse = ", ")))
        }
            
        ## Restructure modelProduction for saving
        if(!is.null(modelProduction)){
            ## If not NULL, extract needed info.  Otherwise, if NULL (i.e. 
            ## model failed) don't do anything (as saving a dataset with
            ## NULL shouldn't cause problems).
            modelProduction = modelProduction[timePointYears <= endYear &
                                              timePointYears >= startYear &
                                              geographicAreaM49 %in% countryM49, ]
            modelProduction[, Value := sapply(Value, roundResults)]
        }
            
        if(!is.null(modelYield)){
            ## See comment for modelProduction
            modelYield = modelYield[timePointYears <= endYear &
                                    timePointYears >= startYear &
                                    geographicAreaM49 %in% countryM49, ]
        }

        dataToSave = rbind(modelYield, modelProduction)
        ## This approach of saving the computed yield results doesn't work when
        ## we are required to choose between two different models (with and
        ## without estimates).
#         if(exists("modelComputeYield")){
#             modelComputeYield = modelComputeYield[
#                 timePointYears <= endYear & timePointYears >= startYear &
#                 geographicAreaM49 %in% countryM49, ]
#             dataToSave = rbind(dataToSave, modelComputeYield)
#         }
        ## HACK: Update China and Pacific
        warning("Hack below!  Remove once the geographicAreaM49 dimension is fixed!")
        dataToSave = dataToSave[!geographicAreaM49 %in% c("1249", "156", "582"), ]
        dataToSave = dataToSave[!is.na(Value), ]
        if((!is.null(dataToSave)) && nrow(dataToSave) > 0){
            saveProductionData(data = dataToSave,
                    areaHarvestedCode = formulaTuples[i, input],
                    yieldCode = formulaTuples[i, productivity],
                    productionCode = formulaTuples[i, output],
                    normalized = TRUE)
        }
        successCount = successCount + 1
    }
}

itemCnt = length(swsContext.datasets[[1]]@dimensions$measuredItemCPC@keys)
paste0("Imputation completed with ", successCount,
       " commodities imputed out of ", itemCnt, " commodities.")
