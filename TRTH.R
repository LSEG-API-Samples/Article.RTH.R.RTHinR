library(jsonlite)
library(httr)
library(readr)

TRTHLogin <- function(uname,pword) {
    url <- "https://hosted.datascopeapi.reuters.com/RestApi/v1/Authentication/RequestToken"
    b <- list(Credentials=list(Username=jsonlite::unbox(uname),Password=jsonlite::unbox(pword)))
    r <- httr::POST(url,add_headers(prefer = "respond-async"),content_type_json(),body = b,encode = "json")
    stop_for_status(r)
    a <- httr::content(r, "parsed", "application/json", encoding="UTF-8")
    token <- paste('Token',a[[2]],sep=" ")
    return(token)
}

TRTHUserInfo <- function(token,uname) {
    url <- paste0("https://hosted.datascopeapi.reuters.com/RestApi/v1/Users/Users(",uname,")")
    r <- GET(url,add_headers(prefer = "respond-async",Authorization = token))
    stop_for_status(r)
    a<-content(r, "parsed", "application/json", encoding="UTF-8")
    return(a)
}

TRTHUserPackages <- function(token) {
    url <- "https://hosted.datascopeapi.reuters.com/RestApi/v1/StandardExtractions/UserPackages"
    r <- GET(url,add_headers(prefer = "respond-async",Authorization = token))
    stop_for_status(r)
    a<-content(r, "parsed", "application/json", encoding="UTF-8")
    return(a)
}

TRTHPackageDeliveriesByPackageId <- function(token,PackageId) {
    url <- paste0("https://hosted.datascopeapi.reuters.com/RestApi/v1/StandardExtractions/UserPackageDeliveryGetUserPackageDeliveriesByPackageId(PackageId='",PackageId,"')")
    r <- GET(url,add_headers(prefer = "respond-async",Authorization = token))
    stop_for_status(r)
    a<-content(r, "parsed", "application/json", encoding="UTF-8")
    return(a)
}

TRTHGetUserPackageDeliveries <- function(token,PackageId) {
    url <- paste0("https://hosted.datascopeapi.reuters.com/RestApi/v1/StandardExtractions/UserPackageDeliveries('",PackageId,"')/$value")
    r <- GET(url,add_headers(prefer = "respond-async",Authorization = token))
    stop_for_status(r)
    a<-content(r, "parsed", "text/csv", encoding="UTF-8")
    return(a)
}