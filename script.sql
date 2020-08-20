#------------------------------------------------------------------------------------------------------#
# Script Name: DDL
# Script Designed On: 15/11/2016
# Script Designed On: 15/11/2016
# Script Descrption: This script is used for twitter data analytics.
#------------------------------------------------------------------------------------------------------#



CREATE TABLE t_search_staging
(
search_Id int IDENTITY(100,1) PRIMARY KEY,
search_String  varchar(255),
no_of_tweets int,
tweet_lang varchar(5),
since varchar(20),
until varchar(20),
geocode varchar(100),
search_status varchar(10) default 'QUEUED'
);

alter procedure InsertSearchCritStaging
(
@search_String  varchar(255),
@no_of_tweets int,
@tweet_lang varchar(5),
@since varchar(20),
@until varchar(20),
@geocode varchar(100),
@id  int OUTPUT
) 
As
 Begin

 BEGIN TRY
 DECLARE @status int;
 SET NOCOUNT ON;
   Insert into t_search_staging (search_String ,no_of_tweets,tweet_lang,since,until,geocode)
   Values(@search_String ,@no_of_tweets,@tweet_lang,@since,@until,@geocode)
   
   set @status = (select search_Id from t_search_staging where search_String = @search_String)
   set @id =  @status

   END TRY
   BEGIN CATCH
   DECLARE @ErrorNumber VARCHAR(MAX)  
  DECLARE @ErrorState VARCHAR(MAX)  
  DECLARE @ErrorSeverity VARCHAR(MAX)  
  DECLARE @ErrorLine VARCHAR(MAX)  
  DECLARE @ErrorProc VARCHAR(MAX)  
  DECLARE @ErrorMesg VARCHAR(MAX)  
  DECLARE @vUserName VARCHAR(MAX)  
  DECLARE @vHostName VARCHAR(MAX) 

  SELECT  @ErrorNumber = ERROR_NUMBER()  
       ,@ErrorState = ERROR_STATE()  
       ,@ErrorSeverity = ERROR_SEVERITY()  
       ,@ErrorLine = ERROR_LINE()  
       ,@ErrorProc = ERROR_PROCEDURE()  
       ,@ErrorMesg = ERROR_MESSAGE()  
       ,@vUserName = SUSER_SNAME()  
       ,@vHostName = Host_NAME()  
  
INSERT INTO ExceptionTracer(vErrorNumber,vErrorState,vErrorSeverity,vErrorLine,
	vErrorProc,vErrorMsg,vUserName,vHostName,dErrorDate)  
VALUES(@ErrorNumber,@ErrorState,@ErrorSeverity,@ErrorLine,@ErrorProc,
	@ErrorMesg,@vUserName,@vHostName,GETDATE()) 
	
	set @id =  0 
   END CATCH

   PRINT 'Hello! There is some error, please check last entry in ExceptionTracer table! '
   End



/*------- Below Code is to Insert the Search Data ...............*/
  DECLARE @GetOutput int;
    exec InsertSearchCritStaging 
    @search_String ='@narendramodi' ,
	@no_of_tweets = 50,
	@tweet_lang='en',
	@since = '2016-01-01',
	@until ='2016-11-01',
	@geocode = '20.593684,78.96288,100km',
	@id = @GetOutput Output 
/*...............................................................*/

-- To catch the exception.
CREATE TABLE ExceptionTracer
(
  iErrorID INT PRIMARY KEY IDENTITY(1,1),
  vErrorNumber INT,
  vErrorState INT,
  vErrorSeverity INT,
  vErrorLine INT,
  vErrorProc VARCHAR(MAX),
  vErrorMsg VARCHAR(MAX),
  vUserName VARCHAR(MAX),
  vHostName VARCHAR(MAX),
  dErrorDate DATETIME DEFAULT GETDATE()
);
