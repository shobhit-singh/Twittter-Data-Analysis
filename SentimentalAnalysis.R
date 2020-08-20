#------------------------------------------------------------------------------------------------------#
# R Script Name: Sentimental Analysis.R
# R Script Designed On: 30/9/2016
# R Script Designed On: 30/9/2016
# R Script Descrption: This script is used to calculate sentimental scores of the extracted tweets.
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

count<- sqlQuery(my_conn, "select count(1) from t_search_process_status where process_status = 'Queued' and process_description in ('Sentimental Analysis using R scripts','Sentimental Analysis Plot') ")


if(count==2){
  
  count_sid<- sqlQuery(my_conn, "select search_id from t_search_process_status where process_status = 'Queued' and process_description='Sentimental Analysis using R scripts' ")
  
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
  
  Search_id_request<- sqlQuery(my_conn, "select search_Id from t_search_process_status  where process_status='QUEUED' and process_description='Sentimental Analysis using R scripts' ")
  
  file_location<- paste("C:\\Users\\lenovo\\Desktop\\Logs\\SentimentalAnalysisScriptLogs\\", paste("Logs_SearchId_",Search_id_request,"_",Systime, ".txt", sep=""),sep="")
  log_con <- file(file_location, open="a")
  
  text <- paste("RODBC library has been imported sucesfully at: ",Sys.time(), sep=" ")
  cat( text , file = log_con, sep="\n", append=FALSE)
  text <- "......................................................................................."
  cat( text , file = log_con, sep="\n", append=FALSE)
  
  
  text <- paste("Imorting CSV cleaned tweets data file from location of Search Id:",Search_id_request," at:",Sys.time(), sep=" ")
  cat( text , file = log_con, sep="\n", append=FALSE)
  text <- "......................................................................................."
  cat( text , file = log_con, sep="\n", append=FALSE)
  
  x<- "C:\\Users\\lenovo\\Documents\\Visual Studio 2015\\Projects\\WebApplication1\\WebApplication1\\Output\\"
  
  # Hardcode
  # Search_id_request = 114
  
  y<- paste0(x,"Ouput_Search_Id_",Search_id_request)
  
  if(!dir.exists(y)){
    
    dir.create(y) 
    
  }
  
  z<- paste0(y,"\\S_ID_",Search_id_request,"_Cleaned_Tweets_Collected.csv")
  
  twwets_df <- read.csv(file=z, header=TRUE, sep=",",colClasses = "character")
  
  
  text <- paste("CSV file has been impoted for Search Id:",Search_id_request," at:",Sys.time(), sep=" ")
  cat( text , file = log_con, sep="\n", append=FALSE)
  
  text <- "......................................................................................."
  cat( text , file = log_con, sep="\n", append=FALSE)
  
  text <- paste("Sentimental Analysis has been started for data having Search Id:",Search_id_request," at:",Sys.time(), sep=" ")
  cat( text , file = log_con, sep="\n", append=FALSE)
  
  text <- "......................................................................................."
  cat( text , file = log_con, sep="\n", append=FALSE)
  
  twwets_df<- subset(twwets_df, select = c(text))

  # Import Positive and Negative Words
  pos = readLines("C:\\Users\\lenovo\\Documents\\Visual Studio 2015\\Projects\\WebApplication1\\WebApplication1\\R Files\\positive_words.txt")
  neg = readLines("C:\\Users\\lenovo\\Documents\\Visual Studio 2015\\Projects\\WebApplication1\\WebApplication1\\R Files\\negative_words.txt")
  
  #dump("my_score_function", file="C:\\Users\\lenovo\\Documents\\score_sentiment.R")
  
  source("C:\\Users\\lenovo\\Documents\\Visual Studio 2015\\Projects\\WebApplication1\\WebApplication1\\R Files\\score_sentiment.R")
  
  # Apply function score.sentiment
  scores = score.sentiment(twwets_df$text, pos, neg, .progress='text')
  scores["Search_id"] = Search_id_request
  rownames(scores) <- NULL
  
  my_conn <- odbcConnect("RSQL", uid="sa", pwd="**")
  sqlSave(my_conn, scores, tablename = "t_search_output_sentmnt_score",rownames = FALSE, append = TRUE)
  
  my_query <- paste("select search_String from t_search_staging  where  search_status='InProgress' and search_Id=",Search_id_request, sep="")
  
  Search_string_request <- sqlQuery(my_conn, my_query)
  
  scores["search_String"] = Search_string_request

  
  x<- "C:\\Users\\lenovo\\Documents\\Visual Studio 2015\\Projects\\WebApplication1\\WebApplication1\\Output\\"
  
  y<- paste0(x,"Ouput_Search_Id_",Search_id_request)
  
  if(!dir.exists(y)){
    dir.create(y) 
  }
  # Saving Sentiments score in project location
  # Sentimental Analysis Bar Plot location = z.
  z<- paste0(y,"\\S_ID_",Search_id_request,"_Sentiments_Score.csv")
  
  write.csv(file=z, x=scores)
  
  
  my_query <- paste("select search_String from t_search_staging  where  search_status='InProgress' and search_Id=",Search_id_request, sep="")
  
  Search_string_request <- sqlQuery(my_conn, my_query)

  scores$score = as.numeric(as.character(scores$score)) 
  scores["Sentiments"] = NA
  
  scores[scores$score> 0, "Sentiments"] = "Positive"
  scores[scores$score< 0, "Sentiments"] = "Negative"
  scores[scores$score == 0, "Sentiments"] = "Neutral"
  
  scores_aggregate <- aggregate(cbind(count = Sentiments) ~ Sentiments, 
            data = scores, 
            FUN = function(x){NROW(x)})
  rownames(scores_aggregate) <- NULL
  
  scores_aggregate$count = as.numeric(as.character(scores_aggregate$count)) 
  
  x<- "C:\\Users\\lenovo\\Documents\\Visual Studio 2015\\Projects\\WebApplication1\\WebApplication1\\Output\\"
  
  y<- paste0(x,"Ouput_Search_Id_",Search_id_request)
  
  if(!dir.exists(y)){
    dir.create(y) 
  }
  
  z1<- paste0(y,"\\S_ID_",Search_id_request,"_Sentiments_Bar_Plot.png")
  
  #png(z1, width=800,height=600)
  # Sentimental Analysis Bar Plot location = z1.
  ggplot(data=scores_aggregate, aes(x=scores_aggregate$Sentiments, y=scores_aggregate$count)) +
    geom_bar(stat="identity")
  
  ggsave(z1)
  
  #dev.off()
  
  text <- paste("Sentimental Analysis has been done for Search Id:",Search_id_request," at:",Sys.time(), sep=" ")
  cat( text , file = log_con, sep="\n", append=FALSE)
  
  
  
  x<- "C:\\Users\\lenovo\\Documents\\Visual Studio 2015\\Projects\\WebApplication1\\WebApplication1\\Output\\"
  
  # Hardcode
  #Search_id_request = 114
  
  y<- paste0(x,"Ouput_Search_Id_",Search_id_request)
  
  if(!dir.exists(y)){
    
    dir.create(y) 
    
  }
  
  loc <- paste0(y,"\\S_ID_",Search_id_request,"_Frequent_Words.csv")
  
  Freq_Words_df <- read.csv(file=loc, header=TRUE, sep=",",colClasses = "character")
  
  Frequent_Words_df =  subset(Freq_Words_df, select = c(Word))
  
  # Apply function score.sentiment
  
  scores_frq_words = score.sentiment(Frequent_Words_df$Word, pos, neg, .progress='text')
  scores_frq_words["Sentiments"] = NA

  
  scores_frq_words[scores_frq_words$score> 0, "Sentiments"] = "Positive"
  scores_frq_words[scores_frq_words$score< 0, "Sentiments"] = "Negative"
  scores_frq_words[scores_frq_words$score == 0, "Sentiments"] = "Neutral"
  
  
  x<- "C:\\Users\\lenovo\\Documents\\Visual Studio 2015\\Projects\\WebApplication1\\WebApplication1\\Output\\"
  
  y<- paste0(x,"Ouput_Search_Id_",Search_id_request)
  
  if(!dir.exists(y)){
    dir.create(y) 
  }
  
  z2<- paste0(y,"\\S_ID_",Search_id_request,"_Sentiments_Wise_Words.csv")
  

  write.csv(file=z2, x=scores_frq_words)
  
  
  my_query<- paste0("DECLARE @GetOutput int;
                 exec InsertSearchProcessStatus 
                  @search_Id =", Search_id_request ,",
                  @process_description = 'Sentimental Analysis Words Lists',
                  @process_status = 'Queued',
                  @location = '",NA,"',
                  @id = @GetOutput Output 
                  select @GetOutput;")
  
  sqlQuery(my_conn,  my_query)
  
  
  text <- paste("Sentimental Analysis plot has been generated for Search Id:",Search_id_request," at:",Sys.time(), sep=" ")
  cat( text , file = log_con, sep="\n", append=FALSE)
  
  
  text <- "......................................................................................."
  cat( text , file = log_con, sep="\n", append=FALSE)
  
  
  my_query <- paste("update t_search_process_status set process_status ='Completed' , location='",z,"' where process_description = 'Sentimental Analysis using R scripts' and search_Id=",Search_id_request, sep=" ")
  
  sqlQuery(my_conn, my_query)
  
  my_query <- paste("update t_search_process_status set process_status ='Completed' , location='",z1,"' where process_description = 'Sentimental Analysis Plot' and search_Id=",Search_id_request, sep=" ")
  
  sqlQuery(my_conn, my_query)
  
  my_query <- paste("update t_search_process_status set process_status ='Completed' , location='",z2,"' where process_description = 'Sentimental Analysis Words Lists' and search_Id=",Search_id_request, sep=" ")
  
  sqlQuery(my_conn, my_query)
  
  text <- paste("Sentimental Analysis using R Script Sucesfully Completed for Search Id:",Search_id_request," at:",Sys.time(), sep=" ")
  cat( text , file = log_con, sep="\n", append=FALSE)
  
  text <- "......................................................................................."
  cat( text , file = log_con, sep="\n", append=FALSE)
  
 
 my_query<- paste0("DECLARE @GetOutput int;
                 exec InsertSearchProcessStatus 
                  @search_Id =", Search_id_request ,",
                  @process_description = 'Location Analytics using R Script',
                  @process_status = 'Queued',
                  @location = '",NA,"',
                  @id = @GetOutput Output 
                  select @GetOutput;")

 # here is the partially completed update
   #my_query <- paste("update t_search_staging set search_status ='Completed' where search_Id =",Search_id_request, sep=" ")
  
 sqlQuery(my_conn, my_query)
  
  
  remove(list=c( "Search_id_request", "twwets_df"))
  remove(list=c("x", "y", "z","my_query"))
  remove(list=c("z1", "z2", "pos","neg","loc"))
  remove(list=c("Freq_Words_df", "Frequent_Words_df", "scores","scores_aggregate","scores_frq_words","Search_string_request"))
  close(log_con)
  close(my_conn)
}else if(flag == 0){
  file_location<- paste("C:\\Users\\lenovo\\Desktop\\Logs\\SentimentalAnalysisScriptLogs\\", paste("Logs_SearchId_",Systime, ".txt", sep=""),sep="")
  log_con <- file(file_location, open="a")
  
  text <- paste("Terminating Sentimental script as no request is queue.", sep=" ")
  cat( text , file = log_con, sep="\n", append=FALSE)
  close(log_con)
  
  
}else{
  file_location<- paste("C:\\Users\\lenovo\\Desktop\\Logs\\SentimentalAnalysisScriptLogs\\", paste("Logs_SearchId_",Systime, ".txt", sep=""),sep="")
  log_con <- file(file_location, open="a")
  
  text <- paste("Terminating Sentimental script as no request is queue", sep=" ")
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