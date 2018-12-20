##' Send email after having checked if some protected figure of production has been changed 
##' 
##' This function simply snd email to the user, 
##' with the result of the check on shares
##' 
##' @param fileToSend If existing, are the SUA of the commodity for which
##' the change of the production figure was required in order to Fill the SUA
##' @param name namefile
##' @param textBody content of the message
##' 
##' @return The function doesn't return anything, but send mail 
##' 
##' @export

sendMailAttachment=function(fileToSend,name,textBody){
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
            dir.create(basedir)
            destfile <- file.path(basedir, paste0(basename, FILETYPE))
            
            # create the csv in a temporary foldes   
            write.csv(fileToSend, destfile, row.names = FALSE)  
            # define on exit strategy
            on.exit(file.remove(destfile))    
            zipfile <- paste0(destfile, ".zip")
            withCallingHandlers(zip(zipfile, destfile, flags = "-j9X"),
                                warning = function(w){
                                    if(grepl("system call failed", w$message)){
                                        stop("The system ran out of memory trying to zip up your data. Consider splitting your request into chunks")
                                    }
                                })
            
            on.exit(file.remove(zipfile), add = TRUE)
            body = textBody
            
            sendmailR::sendmail(from = "sws@fao.org",
                                to = swsContext.userEmail,
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