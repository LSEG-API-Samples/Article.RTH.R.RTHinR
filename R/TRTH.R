library(jsonlite)
library(httr)
library(readr)

cacheEnv <- new.env()

.TRTHGetAllPages <- function(url) {
    token <- get("token",envir = cacheEnv)
    r <- GET(url,add_headers(prefer = "respond-async",Authorization = token))
    stop_for_status(r)
    a<-content(r, "parsed", "application/json", encoding="UTF-8")
    # Check if there is a next link
    if (!is.null(a[["@odata.nextlink"]])) {
        # Call the function again, using next link.
        nurl <- a[["@odata.nextlink"]]
        b<-.TRTHGetAllPages(nurl)
        # Merge the result
        for(i in 1:length(b[["value"]])) {
            a[["value"]][[length(a[["value"]])+1]]<-b[["value"]][[i]]
        }
    }
    # Remove next link to avoid confusion
    a[["@odata.nextlink"]]<-NULL
    return(a)
}

#' Request authentication token
#' @param uname DSS username
#' @param pword DSS password
#' @return An authentication token that must be applied to all requests
TRTHLogin <- function(uname,pword) {
    url <- "https://hosted.datascopeapi.reuters.com/RestApi/v1/Authentication/RequestToken"
    b <- list(Credentials=list(Username=jsonlite::unbox(uname),Password=jsonlite::unbox(pword)))
    r <- httr::POST(url,add_headers(prefer = "respond-async"),content_type_json(),body = b,encode = "json")
    stop_for_status(r)
    a <- httr::content(r, "parsed", "application/json", encoding="UTF-8")
    token <- paste('Token',a[[2]],sep=" ")
    assign("token",token,envir = cacheEnv)
    return(token)
}

#' Set authentication token
#' @param token authentication token
TRTHSetToken <- function(token) {
    assign("token",token,envir = cacheEnv)
}

#' Retrieves a single User information.
#' @param uname DSS username
#' @return Return list of ID, Name, Phone, and Email
TRTHUserInfo <- function(uname) {
    url <- paste0("https://hosted.datascopeapi.reuters.com/RestApi/v1/Users/Users(",uname,")")
    token <- get("token",envir = cacheEnv)
    r <- GET(url,add_headers(prefer = "respond-async",Authorization = token))
    stop_for_status(r)
    a<-content(r, "parsed", "application/json", encoding="UTF-8")
    return(a)
}

#' Retrieve the list of all user packages, i.e. packages to which I am entitled (for all subscriptions)
#' @return Return the list of user package Id, user package name and the corresponding subscription name
TRTHUserPackages <- function() {
    url <- "https://hosted.datascopeapi.reuters.com/RestApi/v1/StandardExtractions/UserPackages"
    a<-.TRTHGetAllPages(url)
    return(a)
}

#' List all user package deliveries (data files) for one package.
#' @param PackageId User package Id. Usually from TRTHUserPackages()
#' @return Return the list of user package delivery Id
TRTHUserPackageDeliveriesByPackageId <- function(packageId) {
    url <- paste0("https://hosted.datascopeapi.reuters.com/RestApi/v1/StandardExtractions/UserPackageDeliveryGetUserPackageDeliveriesByPackageId(PackageId='",packageId,"')")
    a<-.TRTHGetAllPages(url)
    return(a)
}

#' List all user package deliveries (data files) for one package.
#' @param subscriptionId
#' @param fromDate
#' @param toDate
#' @return Returns the list of deliveries by date range
TRTHUserPackageDeliveriesByDateRange <- function(subscriptionId,fromDate,toDate) {
    url <- paste0("https://hosted.datascopeapi.reuters.com/RestApi/v1/StandardExtractions/UserPackageDeliveryGetUserPackageDeliveriesByDateRange(SubscriptionId='",subscriptionId,",FromDate=",fromDate,",ToDate=",toDate,"')")
    a<-.TRTHGetAllPages(url)
    return(a)
}

#' Get the user package delivery and save it to disk
#' @param packageDeliveryId User package delivery Id
#' @param path Path to content to.
#' @param overwrite Will only overwrite ex
TRTHGetUserPackageDeliveries <- function(packageDeliveryId,path,overwrite = FALSE,aws = FALSE) {
    url <- paste0("https://hosted.datascopeapi.reuters.com/RestApi/v1/StandardExtractions/UserPackageDeliveries('",packageDeliveryId,"')/$value")
    token <- get("token",envir = cacheEnv)
    r <- GET(url,add_headers(prefer = "respond-async",Authorization = token),if(aws){add_headers("X-Direct-Download" = "true")},config(http_content_decoding=0,followlocation=0),write_disk(path,overwrite),progress())
    if (status_code(r) == 302) {
      r2 <- GET(r$headers$location,add_headers(prefer = "respond-async"),config(http_content_decoding=0,followlocation=0),write_disk(path,overwrite),progress())
      stop_for_status(r2)
      return(r2)
    }
    stop_for_status(r)
    return(r)
}

#' Search for historical instruments given an instrument identifier.
#' Return instruments may be currently active, or inactive 'historical only' instruments.
#' @param identifier Instrument identifier
#' @param startDateTime The range's start date and time. The format is yyyy-mm-ddThh:mm:ss.sssZ
#' @param endDateTime The range's end date and time. The format is yyyy-mm-ddThh:mm:ss.sssZ
#' @param identifierType The type of identifier. Supported types are Ric, Isin, Cusip, Sedol. Search will look for the identifier in all supported types when not specified.
#' @param resultsBy Determines what information is returned for each found RIC. By RIC: Returns information purely based on the RIC history. By Entity: Returns information based on the entity associated with the RIC on the end date of the Range. This will cause RIC rename history to be returned. Defaults to searching by RIC when not specified.
TRTHHistoricalSearch <- function(identifier,startDateTime,endDateTime,identifierType=NULL,resultsBy=NULL) {
  url <- "https://hosted.datascopeapi.reuters.com/RestApi/v1/Search/HistoricalSearch"
  b <- list(
              Request=list(
                Identifier=identifier,
                Range=list(
                  Start=startDateTime,
                  End=endDateTime
                )
              )
            )
  identifierTypeArg <- match.arg(identifierType,c(NULL,"Ric","Isin","Cusip","Sedol"))
  if (!is.null(identifierType)) {
    b[["Request"]][["IdentifierType"]] <- jsonlite::unbox(identifierTypeArg)
  }
  resultsByArg <- match.arg(resultsBy,c(NULL,"Ric","Entity"))
  if (!is.null(identifierType)) {
    b[["Request"]][["ResultsBy"]] <- jsonlite::unbox(resultsByArg)
  }
  token <- get("token",envir = cacheEnv)
  r <- httr::POST(url,add_headers(prefer = "respond-async",Authorization = token),content_type_json(),body = b,encode = "json")
  warn_for_status(r)
  a<-content(r, "parsed", "application/json", encoding="UTF-8")
  return(a)
}

#' Retrieve FID reference history events for a set of RICs in a specified date range.
#' Returns Collection Of ReferenceHistoryResult
#' @param ricList The RIC identifiers to return reference history for.
#' @param startDateTime The range's start date and time. The format is yyyy-mm-ddThh:mm:ss.sssZ
#' @param endDateTime The range's end date and time. The format is yyyy-mm-ddThh:mm:ss.sssZ
TRTHReferenceHistory <- function(ricList,startDateTime,endDateTime) {
  url <- "https://hosted.datascopeapi.reuters.com/RestApi/v1/Search/ReferenceHistory"
  b <- list(
    Request=list(
      Rics=ricList,
      Range=list(
        Start=jsonlite::unbox(startDateTime),
        End=jsonlite::unbox(endDateTime)
      )
    )
  )
  b <- toJSON(b)
  token <- get("token",envir = cacheEnv)
  r <- httr::POST(url,add_headers(prefer = "respond-async",Authorization = token),content_type_json(),body = b)
  warn_for_status(r)
  a<-content(r, "parsed", "application/json", encoding="UTF-8")
  return(a)
}

#' Returns a list of valid ContentFieldTypes for the report template type.
#' @param reportTemplateTypes Available template type for TRTH are "TickHistoryTimeAndSales","TickHistoryMarketDepth", and "TickHistoryIntradaySummaries".
TRTHGetValidContentFieldTypes <- function(reportTemplateTypes=c("TickHistoryTimeAndSales","TickHistoryMarketDepth","TickHistoryIntradaySummaries")) {
  reportTemplateTypesArg <- match.arg(reportTemplateTypes)
  url <- paste0("https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/GetValidContentFieldTypes(ReportTemplateType=ThomsonReuters.Dss.Api.Extractions.ReportTemplates.ReportTemplateTypes'",reportTemplateTypesArg,"')")
  token <- get("token",envir = cacheEnv)
  r <- GET(url,add_headers(prefer = "respond-async",Authorization = token))
  stop_for_status(r)
  a<-content(r, "parsed", "application/json", encoding="UTF-8")
  return(a)
}

#' Performs an on demand extraction returning the raw results as a stream if the response is available in a short amount of time,
#' otherwise the server accepted the extracting and response with a monitor URL.
#' In the later case, You must poll the extraction status with TRTHCheckRequestStatus.
#'
#' The result format is the native/raw result from the underlying extractor (usually csv).
#' @param b JSON request body. See REST API Reference Tree for format.
#' @param path Path to content to.
#' @param overwrite Will only overwrite existing path if TRUE.
TRTHExtractRaw <- function(b,path,overwrite = FALSE) {
  url <- "https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/ExtractRaw"
  token <- get("token",envir = cacheEnv)
  r <- httr::POST(url,add_headers(prefer = "respond-async",Authorization = token),content_type_json(),body = b,encode = "json")
  if (status_code(r) == 202) {
    message("The request has been accepted but has not yet completed executing asynchronously.\r\nReturn monitor URL\r\n",r$headers$location)
    return(invisible(r$headers$location))
  } else if(status_code(r) == 200) {
    a<-content(r, "parsed", "application/json", encoding="UTF-8")
    message(a$Notes)
    return(TRTHRawExtractionResults(a$JobID,path,overwrite))
  } else {
    warn_for_status(r)
    a<-content(r, "parsed", "application/json", encoding="UTF-8")
    return(a)
  }
}

#' Polling the extraction status.
#' On Demand extraction requests are executed as soon as possible.
#' However, There is no guarantee on the delivery time.
#' If the previous request returned a monitor URL, TRTHCheckRequestStatus must be executed until it returns the result.
#' @param location The monitor URL.
#' @param path Path to content to.
#' @param overwrite Will only overwrite existing path if TRUE.
TRTHCheckRequestStatus <- function(location,path,overwrite = FALSE) {
  token <- get("token",envir = cacheEnv)
  r <- GET(location,add_headers(prefer = "respond-async",Authorization = token))
  if (status_code(r) == 202) {
    message("The request has not yet completed executing asynchronously.\r\nPlease wait a bit and check the request status again.\r\n")
    return(invisible(r$headers$location))
  } else if(status_code(r) == 200) {
    a<-content(r, "parsed", "application/json", encoding="UTF-8")
    message(a$Notes)
    return(TRTHRawExtractionResults(a$JobId,path,overwrite))
  } else {
    warn_for_status(r)
    a<-content(r, "parsed", "application/json", encoding="UTF-8")
    return(a)
  }
}

TRTHRawExtractionResults <- function(jobId,path,overwrite = TRUE) {
  url <- paste0("https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/RawExtractionResults('",jobId,"')/$value")
  token <- get("token",envir = cacheEnv)
  r <- GET(url,add_headers(prefer = "respond-async",Authorization = token),config(http_content_decoding=0),write_disk(path,overwrite),progress())
  stop_for_status(r)
  return(r)
}
