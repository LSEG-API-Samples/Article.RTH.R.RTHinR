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
TRTHUserPackageDeliveriesByPackageId <- function(PackageId) {
    url <- paste0("https://hosted.datascopeapi.reuters.com/RestApi/v1/StandardExtractions/UserPackageDeliveryGetUserPackageDeliveriesByPackageId(PackageId='",PackageId,"')")
    a<-.TRTHGetAllPages(url)
    return(a)
}

#' List all user package deliveries (data files) for one package.
#' @param SubscriptionId
#' @param FromDate
#' @param ToDate
#' @return Returns the list of deliveries by date range
TRTHUserPackageDeliveriesByDateRange <- function(SubscriptionId,FromDate,ToDate) {
    url <- paste0("https://hosted.datascopeapi.reuters.com/RestApi/v1/StandardExtractions/UserPackageDeliveryGetUserPackageDeliveriesByDateRange(SubscriptionId='",SubscriptionId,",FromDate=",FromDate,",ToDate=",ToDate,"')")
    a<-.TRTHGetAllPages(url)
    return(a)
}

#' Get the user package delivery and save it to disk
#' @param PackageDeliveryId User package delivery Id
#' @param Path Path to content to.
#' @param Overwrite Will only overwrite ex
TRTHGetUserPackageDeliveries <- function(PackageDeliveryId,Path,Overwrite = FALSE) {
    url <- paste0("https://hosted.datascopeapi.reuters.com/RestApi/v1/StandardExtractions/UserPackageDeliveries('",PackageDeliveryId,"')/$value")
    token <- get("token",envir = cacheEnv)
    r <- GET(url,add_headers(prefer = "respond-async",Authorization = token),config(http_content_decoding=0),write_disk(Path,Overwrite),progress())
    stop_for_status(r)
    return(r)
}

