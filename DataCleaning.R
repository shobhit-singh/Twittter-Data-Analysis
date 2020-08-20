#------------------------------------------------------------------------------------------------------#
# R Script Name: DataCleaning.R
# R Script Designed On: 8/9/2016
# R Script Designed On: 18/9/2016
# R Script Descrption: This script clean the collected data from twitter API.
#------------------------------------------------------------------------------------------------------#
#----------Importing Required library----------#
library("RODBC", lib.loc="~/R/win-library/3.3")
library("stringi", lib.loc="~/R/win-library/3.3")
library("stringr", lib.loc="~/R/win-library/3.3")
library("XML", lib.loc="~/R/win-library/3.3")
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

count<- sqlQuery(my_conn, "select count(1) from t_search_process_status where process_status = 'Queued' and process_description='Data Cleaning using R Script' ")


if(count==1){
  
  count_sid<- sqlQuery(my_conn, "select search_id from t_search_process_status where process_status = 'Queued' and process_description='Data Cleaning using R Script' ")
  
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
  
  
  Search_id_request<- sqlQuery(my_conn, "select search_Id from t_search_process_status  where process_status='QUEUED' and process_description='Data Cleaning using R Script' ")
 
   file_location<- paste("C:\\Users\\lenovo\\Desktop\\Logs\\DataCleaningScriptLogs\\", paste("Logs_SearchId_",Search_id_request,"_",Systime, ".txt", sep=""),sep="")
  log_con <- file(file_location, open="a")
  
  text <- paste("RODBC library has been imported sucesfully at: ",Sys.time(), sep=" ")
  cat( text , file = log_con, sep="\n", append=FALSE)
  text <- "......................................................................................."
  cat( text , file = log_con, sep="\n", append=FALSE)

  
  my_query <- paste("select location from t_search_process_status where process_status = 'Completed' and search_Id =",Search_id_request, sep=" ")
  
  result<- sqlQuery(my_conn, my_query)
  
  text <- paste("Imorting CSV file from location of Search Id:",Search_id_request," at:",Sys.time(), sep=" ")
  cat( text , file = log_con, sep="\n", append=FALSE)
  text <- "......................................................................................."
  cat( text , file = log_con, sep="\n", append=FALSE)
  #loc<- "C:\\Users\\lenovo\\Desktop\\Output\\Ouput_Search_Id_100\\S_ID_100_Orig_Tweets_collected.csv"

  twwets_df <- read.csv(file=as.character(result$location), header=TRUE, sep=",",colClasses = "character")

   
   text <- paste("CSV file has been impoted for Search Id:",Search_id_request," at:",Sys.time(), sep=" ")
   cat( text , file = log_con, sep="\n", append=FALSE)
   
   text <- "......................................................................................."
   cat( text , file = log_con, sep="\n", append=FALSE)
   
   text <- paste("Cleaning started for data having Search Id:",Search_id_request," at:",Sys.time(), sep=" ")
   cat( text , file = log_con, sep="\n", append=FALSE)
  
    text <- "......................................................................................."
   cat( text , file = log_con, sep="\n", append=FALSE)
   


#twwets_df <- do.call("rbind", lapply(MyData, as.data.frame))
parsedstatusSource <- htmlParse(twwets_df$statusSource)

#step1 <- getNodeSet(parsed, "//a")
#authors <- lapply(step1, xpathSApply, "//a", xmlValue)
#twwets_df["Source"] <- authors[1]

getnode_a <- parsedstatusSource["//a", fun = xmlValue]

final_status_data <- data.frame(matrix(unlist(getnode_a),  byrow=T))

twwets_df["NewStatusSource"] <- final_status_data

twwets_df_keep <- subset(twwets_df, select = c(text, id, NewStatusSource, retweetCount ))

remove(list=c("final_status_data", "twwets_df","parsedstatusSource","getnode_a"))


Encoding(twwets_df_keep$text) <- "UTF-8"
twwets_df_keep$text<- iconv(twwets_df_keep$text, "UTF-8", "UTF-8",sub='')

twwets_df_keep$text = gsub("&amp", "", twwets_df_keep$text)
twwets_df_keep$text = gsub("(RT|via)((?:\\b\\W*@\\w+)+)", " ",  twwets_df_keep$text)
twwets_df_keep$text = gsub("@\\w+", "", twwets_df_keep$text)
twwets_df_keep$text = gsub("[[:punct:]]", "", twwets_df_keep$text)
twwets_df_keep$text = gsub("[[:digit:]]", "", twwets_df_keep$text)
twwets_df_keep$text = gsub("http\\w+", "", twwets_df_keep$text)
twwets_df_keep$text = gsub("[ \t]{2,}", "", twwets_df_keep$text)
twwets_df_keep$text = gsub("^\\s+|\\s+$", "", twwets_df_keep$text)

# clean_tweet<- as.data.frame(clean_tweet)

twwets_df_keep["Search_Id"]<- Search_id_request

twwets_df_keep["DuplicateFlag"]<- duplicated(twwets_df_keep$text)

twwets_df_keep<- subset(twwets_df_keep, twwets_df_keep$DuplicateFlag=="FALSE")

twwets_df_keep<- subset(twwets_df_keep, select = -c(DuplicateFlag))


my_conn <- odbcConnect("RSQL", uid="sa", pwd="**")
sqlSave(my_conn, twwets_df_keep, tablename = "t_search_output_staging", append = TRUE)
#cmd <- paste("insert into t_search_output_staging values ", values)
#remove(list=c("twwets_df", "result", "count"))

x<- "C:\\Users\\lenovo\\Documents\\Visual Studio 2015\\Projects\\WebApplication1\\WebApplication1\\Output\\"


y<- paste0(x,"Ouput_Search_Id_",Search_id_request)

if(!dir.exists(y)){
  
  dir.create(y) 
  
}

z<- paste0(y,"\\S_ID_",Search_id_request,"_Cleaned_Tweets_Collected.csv")

write.csv(file=z, x=twwets_df_keep)


text <- paste("Cleaned data exported in csv at: ",z," at:",Sys.time(), sep=" ")
cat( text , file = log_con, sep="\n", append=FALSE)

text <- "......................................................................................."
cat( text , file = log_con, sep="\n", append=FALSE)


my_query <- paste("update t_search_process_status set process_status ='Completed' , location='",z,"' where process_description = 'Data Cleaning using R Script' and search_Id=",Search_id_request, sep=" ")

sqlQuery(my_conn, my_query)

text <- paste("Cleaning Completed for Search Id:",Search_id_request," at:",Sys.time(), sep=" ")
cat( text , file = log_con, sep="\n", append=FALSE)

text <- "......................................................................................."
cat( text , file = log_con, sep="\n", append=FALSE)

query1<- paste0("DECLARE @GetOutput int;
                 exec InsertSearchProcessStatus 
                @search_Id =", Search_id_request ,",
                @process_description = 'Frequent Words Extraction',
                @process_status = 'Queued',
                @location = '",NA,"',
                @id = @GetOutput Output 
                select @GetOutput;")

sqlQuery(my_conn,  query1)

query1<- paste0("DECLARE @GetOutput int;
                 exec InsertSearchProcessStatus 
                @search_Id =", Search_id_request ,",
                @process_description = 'WordCloud Formation',
                @process_status = 'Queued',
                @location = '",NA,"',
                @id = @GetOutput Output 
                select @GetOutput;")

sqlQuery(my_conn,  query1)

remove(list=c("result", "Search_id_request", "twwets_df_keep","query1"))
remove(list=c("x", "y", "z","my_query"))
close(log_con)
close(my_conn)
}else if(flag == 0){
  file_location<- paste("C:\\Users\\lenovo\\Desktop\\Logs\\DataCleaningScriptLogs\\", paste("Logs_SearchId_",Systime, ".txt", sep=""),sep="")
  log_con <- file(file_location, open="a")
  
  text <- paste("Terminating cleaning script as no request is queue", sep=" ")
  cat( text , file = log_con, sep="\n", append=FALSE)
  close(log_con)
  
}else{
file_location<- paste("C:\\Users\\lenovo\\Desktop\\Logs\\DataCleaningScriptLogs\\", paste("Logs_SearchId_",Systime, ".txt", sep=""),sep="")
log_con <- file(file_location, open="a")

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