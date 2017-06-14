library(jsonlite)
library(httr)
library(readr)

cacheEnv <- new.env()

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
    rtoken <- paste('Token',a[[2]],sep=" ")
    assign("token",rtoken,envir = cacheEnv)
    return(rtoken)
}

#' Retrieves a single User information.
#' @param uname DSS username
#' @return Return list of ID, Name, Phone, and Email
TRTHUserInfo <- function(uname) {
    url <- paste0("https://hosted.datascopeapi.reuters.com/RestApi/v1/Users/Users(",uname,")")
    rtoken <- get("token",envir = cacheEnv)
    r <- GET(url,add_headers(prefer = "respond-async",Authorization = rtoken))
    stop_for_status(r)
    a<-content(r, "parsed", "application/json", encoding="UTF-8")
    return(a)
}

#' Retrieve the list of all user packages, i.e. packages to which I am entitled (for all subscriptions)
#' @return Return the list of user package Id, user package name and the corresponding subscription name
TRTHUserPackages <- function() {
    url <- "https://hosted.datascopeapi.reuters.com/RestApi/v1/StandardExtractions/UserPackages"
    rtoken <- get("token",envir = cacheEnv)
    r <- GET(url,add_headers(prefer = "respond-async",Authorization = rtoken))
    stop_for_status(r)
    a<-content(r, "parsed", "application/json", encoding="UTF-8")
    return(a)
}

#' List all user package deliveries (data files) for one package.
#' @param PackageId User package Id. Usually from TRTHUserPackages()
#' @return Return the list of user package delivery Id
TRTHUserPackageDeliveriesByPackageId <- function(PackageId) {
    url <- paste0("https://hosted.datascopeapi.reuters.com/RestApi/v1/StandardExtractions/UserPackageDeliveryGetUserPackageDeliveriesByPackageId(PackageId='",PackageId,"')")
    rtoken <- get("token",envir = cacheEnv)
    r <- GET(url,add_headers(prefer = "respond-async",Authorization = rtoken))
    stop_for_status(r)
    a<-content(r, "parsed", "application/json", encoding="UTF-8")
    return(a)
}

#' Get the user package delivery
#' @param PackageDeliveryId User package delivery Id
#' @return The package csv file
TRTHGetUserPackageDeliveries <- function(PackageDeliveryId) {
    url <- paste0("https://hosted.datascopeapi.reuters.com/RestApi/v1/StandardExtractions/UserPackageDeliveries('",PackageDeliveryId,"')/$value")
    rtoken <- get("token",envir = cacheEnv)
    r <- GET(url,add_headers(prefer = "respond-async",Authorization = rtoken))
    stop_for_status(r)
    a<-content(r, "parsed", "text/csv", encoding="UTF-8")
    return(a)
}

