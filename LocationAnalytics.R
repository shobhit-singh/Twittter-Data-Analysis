#------------------------------------------------------------------------------------------------------#
# R Script Name: Location_Analytics.R
# R Script Designed On: 15/11/2016
# R Script Designed On: 15/11/2016
# R Script Descrption: This script is used for location analytics of the extracted tweets.
#------------------------------------------------------------------------------------------------------#
#-- Importing Required library
library("RODBC", lib.loc="~/R/win-library/3.3")
library("stringi", lib.loc="~/R/win-library/3.3")
library("stringr", lib.loc="~/R/win-library/3.3")
library("tm", lib.loc="~/R/win-library/3.3")
library("RColorBrewer", lib.loc="~/R/win-library/3.3")
library("wordcloud", lib.loc="~/R/win-library/3.3")
library("plyr", lib.loc="~/R/win-library/3.3")
library("ggplot2", lib.loc="~/R/win-library/3.3")
library("twitteR", lib.loc="~/R/win-library/3.3")
library("leaflet", lib.loc="~/R/win-library/3.3")
library("htmlwidgets", lib.loc="~/R/win-library/3.3")


#-- Script Begin
Systime <- Sys.time() 
Systime<- str_replace_all(Systime,"[[:punct:]]","_")
Systime<- str_replace_all(Systime," ","_")

my_conn <- odbcConnect("RSQL", uid="sa", pwd="**")


inprogress_count<- sqlQuery(my_conn, "select count(1) from t_search_staging where search_status = 'InProgress' ")

if(inprogress_count==1){
  
  inprogress_sid <- sqlQuery(my_conn, "select search_id from t_search_staging where search_status = 'InProgress' ") 
  
}else{
  inprogress_sid<- -2
}

count<- sqlQuery(my_conn, "select count(1) from t_search_process_status where process_status = 'Queued' and process_description in ('Location Analytics using R Script') ")


if(count>=1){
  
  count_sid<- sqlQuery(my_conn, "select search_id from t_search_process_status where process_status = 'Queued' and process_description='Location Analytics using R Script' ")
  
}else{
  count_sid<- -1
}

if(count_sid==inprogress_sid){
  flag <- 1 
}else{
  flag <- 0
}


if(flag==1)
{
  
  Search_id_request<- sqlQuery(my_conn, "select search_Id from t_search_process_status  where process_status='QUEUED' and process_description='Location Analytics using R Script' ")
  
  file_location<- paste("C:\\Users\\lenovo\\Desktop\\Logs\\LocationAnalysisScriptLogs\\", paste("Logs_SearchId_",Search_id_request,"_",Systime, ".txt", sep=""),sep="")
  log_con <- file(file_location, open="a")
  
  text <- paste("RODBC library has been imported sucesfully at: ",Sys.time(), sep=" ")
  cat( text , file = log_con, sep="\n", append=FALSE)
  text <- "......................................................................................."
  cat( text , file = log_con, sep="\n", append=FALSE)
  
  
  text <- paste("Imorting files from location of Search Id:",Search_id_request," at:",Sys.time(), sep=" ")
  cat( text , file = log_con, sep="\n", append=FALSE)
  text <- "......................................................................................."
  cat( text , file = log_con, sep="\n", append=FALSE)
  
  # Hardcode Importing the File for OAuth
  
  x<- "C:\\Users\\lenovo\\Documents\\Visual Studio 2015\\Projects\\WebApplication1\\WebApplication1\\R Files\\OAuth.csv"
  
  twitter_OAuth <- read.csv(file=x, header=TRUE, sep=",",colClasses = "character")
  
  setup_twitter_oauth(twitter_OAuth$Consumer_API_Key, twitter_OAuth$Consumer_API_Secret, 
                      twitter_OAuth$Access_Token, twitter_OAuth$Access_Token_Secret)
  
  x<- "C:\\Users\\lenovo\\Documents\\Visual Studio 2015\\Projects\\WebApplication1\\WebApplication1\\Output\\"
  
  # Hardcode
  # Search_id_request = 116
  
  y<- paste0(x,"Ouput_Search_Id_",Search_id_request)
  
  if(!dir.exists(y)){
    
    dir.create(y) 
    
  }
  
  z<- paste0(y,"\\S_ID_",Search_id_request,"_Cleaned_Tweets_Collected.csv")
  
  twwets_df <- read.csv(file=z, header=TRUE, sep=",",colClasses = "character")
  
  z<- paste0(y,"\\S_ID_",Search_id_request,"_Orig_Tweets_Collected.csv")
  
  orig_twwets_df <- read.csv(file=z, header=TRUE, sep=",",colClasses = "character")
  
  orig_twwets_df <- subset(orig_twwets_df, select=c(id,screenName))
  
  z<- paste0(y,"\\S_ID_",Search_id_request,"_Sentiments_Score.csv")
  
  twwets_score <- read.csv(file=z, header=TRUE, sep=",",colClasses = "character")
  
  twwets_score <- subset(twwets_score, select=c(text,score))
  
  twwets <- merge(twwets_df,twwets_score,by = "text" )
  
  twwets <- merge(twwets,orig_twwets_df,by = "id" )
  
  remove(list=c( "twwets_df", "twwets_score","orig_twwets_df"))
  
  twwets$score = as.numeric(as.character(twwets$score)) 
  twwets["Sentiments"] = NA
  
  twwets[twwets$score> 0, "Sentiments"] = "Positive"
  twwets[twwets$score< 0, "Sentiments"] = "Negative"
  twwets[twwets$score == 0, "Sentiments"] = "Neutral"
  
  text <- paste("Location Analysis has been started for data having Search Id:",Search_id_request," at:",Sys.time(), sep=" ")
  cat( text , file = log_con, sep="\n", append=FALSE)
  
  text <- "......................................................................................."
  cat( text , file = log_con, sep="\n", append=FALSE)
  

  twwets <- as.data.frame(twwets)
  
  users <- lookupUsers(twwets$screenName)
  
  users_df <- twitteR::twListToDF(users)
  
  users_df <- subset(users_df, select=c(screenName,location))
  
  twwets <- merge(twwets,users_df,by = "screenName" )
  
  twwets["length"] = NA
  
  for(i in 1:nrow(twwets)){
    twwets$length[i] <- nchar(as.character(twwets$location[i]))
    next
  }
  
  twwets <- subset(twwets , twwets$length>0)
  
  
  twwets["lat"]<-NA
  twwets["lon"]<-NA
  
  geocodeAdddress <- function(address) {
    require(RJSONIO)
    url <- "http://maps.google.com/maps/api/geocode/json?address="
    url <- URLencode(paste(url, address, "&sensor=false", sep = ""))
    x <- fromJSON(url, simplify = FALSE)
    if (x$status == "OK") {
      out <- c(x$results[[1]]$geometry$location$lat,
               x$results[[1]]$geometry$location$lng)
    } else {
      out <- NA
    }
    Sys.sleep(0.2)  # API only allows 5 requests per second
    out
  }
  
  
  for(i in 1:nrow(twwets)) {
    
    loc<- geocodeAdddress(twwets$location[i])
    
    twwets$lat[i] <- loc[1]
    twwets$lon[i] <- loc[2]
    
  }
  
  twwets <- subset(twwets , twwets$lat!='NA')
  

  twwets_pos <- subset(twwets , twwets$Sentiments=="Positive")
  twwets_neg <- subset(twwets , twwets$Sentiments=="Negative")
  twwets_nuetral <- subset(twwets , twwets$Sentiments=="Neutral")
  
  
  
  map <- leaflet() %>%
    addCircleMarkers(data=twwets_pos,lat= ~ twwets_pos$lat, lng = ~ twwets_pos$lon, radius = 10, color = '#008000', popup = twwets_pos$text)%>%
    addCircleMarkers(data=twwets_neg,lat= ~ twwets_neg$lat, lng = ~ twwets_neg$lon, radius = 10, color = '#ff0000', popup = twwets_neg$text)%>%
    addCircleMarkers(data=twwets_nuetral,lat= ~ twwets_nuetral$lat, lng = ~ twwets_nuetral$lon, radius = 10, color = '#ffff00', popup = twwets_nuetral$text)%>%
    addTiles()
	
	  x<- "C:\\Users\\lenovo\\Documents\\Visual Studio 2015\\Projects\\WebApplication1\\WebApplication1\\"
  
  # Hardcode
  # Search_id_request = 116
  
  z<- paste0(x,"\\S_ID_",Search_id_request,"_Location_Analytics_Map.html")
  
  map_name <- paste0("S_ID_",Search_id_request,"_Location_Analytics_Map.html")
  
  saveWidget(map, file=z, selfcontained = FALSE)
  
  my_query <- paste("update t_search_process_status set process_status ='Completed' , location='",map_name,"' where process_description = 'Location Analytics using R Script' and search_Id=",Search_id_request, sep=" ")
  
  sqlQuery(my_conn, my_query)

  
  text <- paste("Location Analysis using R Script Sucesfully Completed for Search Id:",Search_id_request," at:",Sys.time(), sep=" ")
  cat( text , file = log_con, sep="\n", append=FALSE)
  
  text <- "......................................................................................."
  cat( text , file = log_con, sep="\n", append=FALSE)
  
  # here is the partially completed update
  my_query <- paste("update t_search_staging set search_status ='Completed' where search_Id =",Search_id_request, sep=" ")
  
  sqlQuery(my_conn, my_query)
  
  
  remove(list=c( "Search_id_request"))
  remove(list=c("x", "y", "z","my_query"))
  remove(list=c("loc"))
  remove(list=c( "map","users","tweets"))
  remove(list=c("tweets_neg", "tweets_pos", "tweets_nuetral","user_df","map_name"))
  close(log_con)
  close(my_conn)
}else if(flag == 0){
  file_location<- paste("C:\\Users\\lenovo\\Desktop\\Logs\\LocationAnalysisScriptLogs\\", paste("Logs_SearchId_",Systime, ".txt", sep=""),sep="")
  log_con <- file(file_location, open="a")
  
  text <- paste("Terminating Location Analytics script as no request is queue.", sep=" ")
  cat( text , file = log_con, sep="\n", append=FALSE)
  close(log_con)
  
  
}else{
  file_location<- paste("C:\\Users\\lenovo\\Desktop\\Logs\\LocationAnalysisScriptLogs\\", paste("Logs_SearchId_",Systime, ".txt", sep=""),sep="")
  log_con <- file(file_location, open="a")
  
  text <- paste("Terminating Location Analytics script as no request is queue", sep=" ")
  cat( text , file = log_con, sep="\n", append=FALSE)
  close(log_con)
}
remove(list=c("file_location", "Systime", "text"))
remove(list=c("inprogress_count", "count"))
temp<- tryCatch({odbcGetInfo(my_conn);TRUE},error=function(...)FALSE)
if (temp){
  close(my_conn)
}

temp<- tryCatch({odbcGetInfo(log_con);TRUE},error=function(...)FALSE)
if (temp){
  close(log_con)
}
remove(list=c("temp","inprogress_sid","count_sid","flag"))