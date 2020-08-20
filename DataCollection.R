#------------------------------------------------------------------------------------------------------#
# R Script Name: DataCollection.R
# R Script Designed on: 8/9/2016
# R Sctipt updated on: 18/9/2016
# R Script Descrption: This script collect the data from twitter API.
#------------------------------------------------------------------------------------------------------#
#-----------Importing Required library---------#
library("stringi", lib.loc="~/R/win-library/3.3")
library("stringr", lib.loc="~/R/win-library/3.3")
library("twitteR", lib.loc="~/R/win-library/3.3")
library("RODBC", lib.loc="~/R/win-library/3.3")
library("XML", lib.loc="~/R/win-library/3.3")
#-- Script Begin
Systime <- Sys.time() 
Systime<- str_replace_all(Systime,"[[:punct:]]","_")
Systime<- str_replace_all(Systime," ","_")



my_conn <- odbcConnect("RSQL", uid="sa", pwd="**")


inprogress_count<- sqlQuery(my_conn, "select count(1) from t_search_staging where search_status = 'InProgress' ")

count<- sqlQuery(my_conn, "select count(1) from t_search_staging where search_status = 'QUEUED' ")

if(inprogress_count==0 & count>=1){
  
  Search_id_request<- sqlQuery(my_conn, "select search_Id from t_search_staging where search_Id = (select min(search_Id) from t_search_staging where search_status='QUEUED')")
  
  file_location<- paste("C:\\Users\\lenovo\\Desktop\\Logs\\DataCollectionScriptLogs\\", paste("Logs_SearchId_",Search_id_request,"_",Systime, ".txt", sep=""),sep="")
  log_con <- file(file_location, open="a")
  
  text <- paste("XML, stringi & string, twitteR & RODBC library has been imported sucesfully at: ",Sys.time(), sep=" ")
  cat( text , file = log_con, sep="\n", append=FALSE)
 
   text <- "......................................................................................."
  cat( text , file = log_con, sep="\n", append=FALSE)
  
  
  text <- paste("SQl Database has been connceted sucesfully at: ",Sys.time(), sep=" ")
  cat( text , file = log_con, sep="\n", append=FALSE)
  
  text <- "......................................................................................."
  cat( text , file = log_con, sep="\n", append=FALSE)
  
  
  my_query <- paste("select * from t_search_staging where search_status = 'QUEUED' and search_Id =",Search_id_request, sep=" ")
  
  result<- sqlQuery(my_conn, my_query)
  
 x<- "C:\\Users\\lenovo\\Documents\\Visual Studio 2015\\Projects\\WebApplication1\\WebApplication1\\R Files\\OAuth.csv"
  
  twitter_OAuth <- read.csv(file=x, header=TRUE, sep=",",colClasses = "character")
  
  setup_twitter_oauth(twitter_OAuth$Consumer_API_Key, twitter_OAuth$Consumer_API_Secret, 
                      twitter_OAuth$Access_Token, twitter_OAuth$Access_Token_Secret)
  
  text <- paste("Twitter API Authorization is done: ",Sys.time(), sep=" ")
  cat( text , file = log_con, sep="\n", append=FALSE)
 
  
  text <- "......................................................................................."
  cat( text , file = log_con, sep="\n", append=FALSE)
  
  
  twwets<- searchTwitter(as.character(result$search_String),n = result$no_of_tweets,
                         lang= result$tweet_lang)
  
  text <- paste("Tweets collected sucesfully: ",Sys.time(), sep=" ")
  cat( text , file = log_con, sep="\n", append=FALSE)
  
  text <- "......................................................................................."
  cat( text , file = log_con, sep="\n", append=FALSE)
  
  x<- "C:\\Users\\lenovo\\Documents\\Visual Studio 2015\\Projects\\WebApplication1\\WebApplication1\\Output\\"

  y<- paste0(x,"Ouput_Search_Id_",Search_id_request)
  
  if(!dir.exists(y)){
    
    dir.create(y) 
    
  }
  
  z<- paste0(y,"\\S_ID_",Search_id_request,"_Orig_Tweets_Collected.csv")
  twwets_df <- do.call("rbind", lapply(twwets, as.data.frame))
  
  write.csv(file=z, x=twwets_df)
  
  
  my_query <- paste("update t_search_staging set search_status ='InProgress' where search_Id =",Search_id_request, sep=" ")
  
  sqlQuery(my_conn, my_query)
  
  
  query1<- paste0("DECLARE @GetOutput int;
                 exec InsertSearchProcessStatus 
                 @search_Id =", Search_id_request ,",
                 @process_description = 'Data Collection from Twitter API',
                 @process_status = 'Completed',
                 @location = '",z,"',
                 @id = @GetOutput Output 
                 select @GetOutput;")
  
  sqlQuery(my_conn,  query1)
  
  query2<- paste0("DECLARE @GetOutput int;
                 exec InsertSearchProcessStatus 
               @search_Id =", Search_id_request ,",
               @process_description = 'Data Cleaning using R Script',
               @process_status = 'Queued',
               @location = ",NA,",
               @id = @GetOutput Output 
               select @GetOutput;")
  
  sqlQuery(my_conn,  query2)
  
  close(my_conn)
  
  #id<- as.data.frame(result$search_Id)  # This the unique Id of Search
  
  #write.csv(file="C:/Users/lenovo/Desktop/Project/Results.csv", x=twwets_df_keep)
  
  #remove(list=c("id", "result","twwets_df_keep","my_query","Search_id_request"))
  remove(list=c("x", "y", "z","Search_id_request","query1","query2"))
  remove(list=c("twwets","result","twwets_df","my_query"))
  
}else if(inprogress_count==1){
  
  file_location<- paste("C:\\Users\\lenovo\\Desktop\\Logs\\DataCollectionScriptLogs\\", paste("R_Logs_",Systime, ".txt", sep=""),sep="")
  log_con <- file(file_location, open="a")
  
  text <- paste("Terminationg R Script:  Data Analysis already is 'InProgress' for previous request!", sep=" ")
  cat( text , file = log_con, sep="\n", append=FALSE)
  
}else if(count==0){
  
  file_location<- paste("C:\\Users\\lenovo\\Desktop\\Logs\\DataCollectionScriptLogs\\", paste("R_Logs_",Systime, ".txt", sep=""),sep="")
  log_con <- file(file_location, open="a")
  
  
  text <- paste("No search request is in 'Queued' for data analysis! Please register request.", sep=" ")
  cat( text , file = log_con, sep="\n", append=FALSE)
}
remove(list=c("inprogress_count", "count"))
remove(list=c("file_location", "Systime", "text"))

temp<- tryCatch({odbcGetInfo(my_conn);TRUE},error=function(...)FALSE)

if (temp){
  close(my_conn)
}
close(log_con)
remove(list=c("temp"))