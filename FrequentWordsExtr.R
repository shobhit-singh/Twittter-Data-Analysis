#------------------------------------------------------------------------------------------------------#
# R Script Name: FrequentWordsExtr.R
# R Script Designed On: 17/9/2016
# R Script Designed On: 18/9/2016
# R Script Descrption: This script is used to extract the frequent words in data extracted for particular search.
#------------------------------------------------------------------------------------------------------#
#-- Importing Required library
library("RODBC", lib.loc="~/R/win-library/3.3")
library("stringi", lib.loc="~/R/win-library/3.3")
library("stringr", lib.loc="~/R/win-library/3.3")
library("tm", lib.loc="~/R/win-library/3.3")
library("RColorBrewer", lib.loc="~/R/win-library/3.3")
library("wordcloud", lib.loc="~/R/win-library/3.3")
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

count<- sqlQuery(my_conn, "select count(1) from t_search_process_status where process_status = 'Queued' and process_description in ('Frequent Words Extraction','WordCloud Formation') ")


if(count==2){
  
  count_sid<- sqlQuery(my_conn, "select search_id from t_search_process_status where process_status = 'Queued' and process_description='Frequent Words Extraction' ")
  
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
  
  Search_id_request<- sqlQuery(my_conn, "select search_Id from t_search_process_status  where process_status='QUEUED' and process_description='Frequent Words Extraction' ")
  
  file_location<- paste("C:\\Users\\lenovo\\Desktop\\Logs\\FrequentWordsScriptLogs\\", paste("Logs_SearchId_",Search_id_request,"_",Systime, ".txt", sep=""),sep="")
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
  
  text <- paste("Frequent Words extraction started for data having Search Id:",Search_id_request," at:",Sys.time(), sep=" ")
  cat( text , file = log_con, sep="\n", append=FALSE)
  
  text <- "......................................................................................."
  cat( text , file = log_con, sep="\n", append=FALSE)
  
  my_corpus<- Corpus(VectorSource(twwets_df)) 
  
  Data<- tm_map(my_corpus, function(x) iconv(enc2utf8(x$content), sub = "bytes"))
  Data<-tm_map(Data,removePunctuation)
  Data <- tm_map(Data, stripWhitespace)
  Data<-tm_map(Data,removeNumbers)
  Data <- tm_map(Data, PlainTextDocument)
  Data <- tm_map(Data, content_transformer(tolower))
  Data <- tm_map(Data, tolower)
  
  myStopwords <- c(stopwords("english"),"twitter", "the", "is" 
                   , "are","with","for","and","our","have","And"
                   ,"My","my","u","U","will","a","all","had","say",
                   "i","must","been","during","you","your","made", 
                   "to", "has","too","was","were","would","can",
                   "could","am","be","many","in","on",
                   "with","as","ur","this","them","that",
                   "those","there","what","we","android","client","keep")
  Data <- tm_map(Data, removeWords, myStopwords)
  Data <- tm_map(Data, PlainTextDocument)
  
  my_tdm<- TermDocumentMatrix(Data)
  m <- as.matrix(my_tdm)
  Words<- sort(rowSums(m), decreasing = TRUE)
  words_df<-data.frame(Word = names(Words), freq=Words)
  
  rownames(words_df) <- NULL
  
  words_df["DuplicateFlag"]<- duplicated(words_df$Word)
  
  words_df<- subset(words_df, words_df$DuplicateFlag=="FALSE")
  
  words_df<- subset(words_df, select = -c(DuplicateFlag))
  
  words_df["WordLength"] <- 0
  
  obs<- length(words_df$Word)
  
  
  for(i in 1:obs){
    words_df[i,3] <- nchar(as.character(words_df$Word[i]))
    next
  }
  
  words_df <- subset(words_df , words_df$WordLength<15)
  
  count<- nrow(words_df)
  
  x<- "C:\\Users\\lenovo\\Documents\\Visual Studio 2015\\Projects\\WebApplication1\\WebApplication1\\Output\\"
  
  y<- paste0(x,"Ouput_Search_Id_",Search_id_request)
  
  if(!dir.exists(y)){
    dir.create(y) 
    }
  
  z<- paste0(y,"\\S_ID_",Search_id_request,"_Word_Cloud_Plot.png")
  
  if(count<=300){
    png(z, width=600,height=400)  
    
  }else if(count>300& count<=500) {
    
    png(z, width=800,height=600)  
    
  }else{
    
    png(z, width=1000,height=800)  
    
  }
  
  pal = brewer.pal(8,"Dark2")
  
  wordcloud(words= words_df$Word, freq=words_df$freq, max.words = 
              500, random.order = FALSE, colors = pal, min.freq = 1,rot.per=.15)
  dev.off()
  
  
  #final data in twwets_df_keep
  #twwets_df_keep<- subset(twwets_df_keep, select = -c(DuplicateFlag))
  
  words_df["Search_Id"]<- Search_id_request
  rownames(words_df) <- NULL
  
  words_df<- subset(words_df, select = -c(WordLength))
  
  my_conn <- odbcConnect("RSQL", uid="sa", pwd="**")
  sqlSave(my_conn, words_df, tablename = "t_search_output_freq_words", append = TRUE)
  
  #cmd <- paste("insert into t_search_output_staging values ", values)
  #remove(list=c("twwets_df", "result", "count"))
  
  z2<- paste0(y,"\\S_ID_",Search_id_request,"_Frequent_Words.csv")
  
  write.csv(file=z2, x=words_df)
  
  
  text <- paste("Frequent Words extraction & wordcloud formation is completed for data having Search Id:",Search_id_request," at:",Sys.time(), sep=" ")
  cat( text , file = log_con, sep="\n", append=FALSE)
  
  text <- "......................................................................................."
  cat( text , file = log_con, sep="\n", append=FALSE)
  
  
  my_query <- paste("update t_search_process_status set process_status ='Completed' , location='",z,"' where process_description = 'WordCloud Formation' and search_Id=",Search_id_request, sep=" ")
  
  sqlQuery(my_conn, my_query)
  
  my_query <- paste("update t_search_process_status set process_status ='Completed' , location='",z2,"' where process_description = 'Frequent Words Extraction' and search_Id=",Search_id_request, sep=" ")
  
  sqlQuery(my_conn, my_query)
  
  text <- paste("Frequent Words extraction & wordcloud formation Script Sucesfully Completed for Search Id:",Search_id_request," at:",Sys.time(), sep=" ")
  cat( text , file = log_con, sep="\n", append=FALSE)
  
  text <- "......................................................................................."
  cat( text , file = log_con, sep="\n", append=FALSE)
  
  # here is the partially completed update
  #my_query <- paste("update t_search_staging set search_status ='Completed' where search_Id =",Search_id_request, sep=" ")
  
  #sqlQuery(my_conn, my_query)
  
  
  query1<- paste0("DECLARE @GetOutput int;
                 exec InsertSearchProcessStatus 
                  @search_Id =", Search_id_request ,",
                  @process_description = 'Sentimental Analysis using R scripts',
                  @process_status = 'Queued',
                  @location = '",NA,"',
                  @id = @GetOutput Output 
                  select @GetOutput;")
  
  sqlQuery(my_conn,  query1)
  
  query1<- paste0("DECLARE @GetOutput int;
                 exec InsertSearchProcessStatus 
                  @search_Id =", Search_id_request ,",
                  @process_description = 'Sentimental Analysis Plot',
                  @process_status = 'Queued',
                  @location = '",NA,"',
                  @id = @GetOutput Output 
                  select @GetOutput;")
  
  sqlQuery(my_conn,  query1)
  
  
  remove(list=c("m", "Search_id_request", "twwets_df","words_df"))
  remove(list=c("x", "y", "z","my_query"))
  close(log_con)
  close(my_conn)
  remove(list=c("my_corpus","my_tdm","pal","Words","z2","i","obs","myStopwords"))
}else if(flag == 0){
  file_location<- paste("C:\\Users\\lenovo\\Desktop\\Logs\\FrequentWordsScriptLogs\\", paste("Logs_SearchId_",Systime, ".txt", sep=""),sep="")
  log_con <- file(file_location, open="a")
  
  text <- paste("Terminating cleaning script as no request is queue.", sep=" ")
  cat( text , file = log_con, sep="\n", append=FALSE)
  close(log_con)
 
  
}else{
  file_location<- paste("C:\\Users\\lenovo\\Desktop\\Logs\\FrequentWordsScriptLogs\\", paste("Logs_SearchId_",Systime, ".txt", sep=""),sep="")
  log_con <- file(file_location, open="a")
  
  text <- paste("Terminating cleaning script as no request is queue", sep=" ")
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