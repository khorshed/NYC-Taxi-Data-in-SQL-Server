SET NOCOUNT ON
USE TaxiData
GO

DECLARE @DataKey VARCHAR(10)='Yellow'

DECLARE 
@CSVFilePath				VARCHAR(100)='H:\Big Data\CSV\' + @DataKey + '\yellow_tripdata_~.csv',
@FileGroup					VARCHAR(100)='FG_' + @DataKey + '~',
@NDFIlePath					VARCHAR(100) ='H:\MSSQL\TaxiData\'+ @dataKey+'\~.ndf',
@NDFFileName				VARCHAR(100)='P_' + @DataKey + '~'

DECLARE @FormatBefore_2015		VARCHAR(1000)='H:\Big Data\FormatFile\Yellow_FormatBefore_2015.xml'
DECLARE @Format2015_2016_B4_07	VARCHAR(1000)='H:\Big Data\FormatFile\Yellow_Format2015_2016_B4_07.xml'
DECLARE @FormatAfter_2016_07	VARCHAR(1000)='H:\Big Data\FormatFile\Yellow_FormatAfter_2016_07.xml'



DECLARE  @FGSQL				VARCHAR(MAX) ='
IF EXISTS(
       SELECT *
       FROM   sys.database_files AS df
       WHERE  df.name = ''!!NDFFileName''
	   )
ALTER DATABASE [TaxiData] REMOVE FILE !!NDFFileName;

IF EXISTS(
	SELECT *
	FROM   sys.filegroups AS f
	WHERE  f.name = ''!!FileGroup'')
ALTER DATABASE TaxiData REMOVE FILEGROUP !!FileGroup;

ALTER DATABASE TaxiData ADD FILEGROUP !!FileGroup',
@NDFSQL						VARCHAR(MAX) =
'ALTER DATABASE TaxiData
    ADD FILE (
    NAME = [!!NDFFileName],
    FILENAME = ''!!NDFIlePath'',
        SIZE = 1024 KB, 
        MAXSIZE = UNLIMITED, 
        FILEGROWTH = 1024 MB
    ) TO FILEGROUP [!!FileGroup];'

DECLARE @StgTableName VARCHAR(100)= @DataKey+'_tripdata_~'

DECLARE @tblTxt	VARCHAR(MAX)=''

SET @tblTxt='
IF OBJECT_ID(''!!StgTableName'') IS NOT NULL
DROP TABLE !!StgTableName;

CREATE TABLE !!StgTableName
(
	ID BIGINT DEFAULT NEXT VALUE FOR Trip_Records_Seq, 
	vendor_id                 VARCHAR(5),
	tpep_pickup_datetime      DATETIME NOT NULL,
	tpep_dropoff_datetime     DATETIME,
	passenger_count           VARCHAR(30),
	trip_distance             VARCHAR(30),
	pickup_longitude          VARCHAR(30),
	pickup_latitude           VARCHAR(30),
	rate_code_id              VARCHAR(20),
	store_and_fwd_flag        VARCHAR(30),
	dropoff_longitude         VARCHAR(30),
	dropoff_latitude          VARCHAR(30),
	payment_type              VARCHAR(20),
	fare_amount               VARCHAR(30),
	extra                     VARCHAR(30),
	mta_tax                   VARCHAR(30),
	tip_amount                VARCHAR(30),
	tolls_amount              VARCHAR(30),
	improvement_surcharge     VARCHAR(30),
	total_amount              VARCHAR(40),
	pickup_location_id        VARCHAR(20),
	dropoff_location_id       VARCHAR(20)
) ON [!!FileGroup];'

DECLARE @ColumnList VARCHAR(MAX)=''

DECLARE @BulkInsert VARCHAR(MAX)=''
SET @BulkInsert='
INSERT INTO !!StgTableName (!!ColumnList)
SELECT !!ColumnList
FROM   OPENROWSET(
           BULK ''!!CSVFilePath'',
           FORMATFILE = ''!!FormatFile'',
           FIRSTROW=2           
       ) AS DATA;'

DECLARE @Partition VARCHAR(MAX)=''
DECLARE @PartitionFunc VARCHAR(MAX)=''
DECLARE @PartitonSche VARCHAR(MAX)=''
DECLARE @CheckCons VARCHAR(MAX)=''
DECLARE @PartitionSwitch VARCHAR(MAX)=''


DECLARE  @mTxt TABLE (txt VARCHAR(100) NOT NULL, id INT NOT NULL)
insert into @mTxt values ('2009-01',1)
insert into @mTxt values ('2009-02',2)
insert into @mTxt values ('2009-03',3)
insert into @mTxt values ('2009-04',4)
insert into @mTxt values ('2009-05',5)
insert into @mTxt values ('2009-06',6)
insert into @mTxt values ('2009-07',7)
insert into @mTxt values ('2009-08',8)
insert into @mTxt values ('2009-09',9)
insert into @mTxt values ('2009-10',10)
insert into @mTxt values ('2009-11',11)
insert into @mTxt values ('2009-12',12)
insert into @mTxt values ('2010-01',13)
insert into @mTxt values ('2010-02',14)
insert into @mTxt values ('2010-03',15)
insert into @mTxt values ('2010-04',16)
insert into @mTxt values ('2010-05',17)
insert into @mTxt values ('2010-06',18)
insert into @mTxt values ('2010-07',19)
insert into @mTxt values ('2010-08',20)
insert into @mTxt values ('2010-09',21)
insert into @mTxt values ('2010-10',22)
insert into @mTxt values ('2010-11',23)
insert into @mTxt values ('2010-12',24)
insert into @mTxt values ('2011-01',25)
insert into @mTxt values ('2011-02',26)
insert into @mTxt values ('2011-03',27)
insert into @mTxt values ('2011-04',28)
insert into @mTxt values ('2011-05',29)
insert into @mTxt values ('2011-06',30)
insert into @mTxt values ('2011-07',31)
insert into @mTxt values ('2011-08',32)
insert into @mTxt values ('2011-09',33)
insert into @mTxt values ('2011-10',34)
insert into @mTxt values ('2011-11',35)
insert into @mTxt values ('2011-12',36)
insert into @mTxt values ('2012-01',37)
insert into @mTxt values ('2012-02',38)
insert into @mTxt values ('2012-03',39)
insert into @mTxt values ('2012-04',40)
insert into @mTxt values ('2012-05',41)
insert into @mTxt values ('2012-06',42)
insert into @mTxt values ('2012-07',43)
insert into @mTxt values ('2012-08',44)
insert into @mTxt values ('2012-09',45)
insert into @mTxt values ('2012-10',46)
insert into @mTxt values ('2012-11',47)
insert into @mTxt values ('2012-12',48)
insert into @mTxt values ('2013-01',49)
insert into @mTxt values ('2013-02',50)
insert into @mTxt values ('2013-03',51)
insert into @mTxt values ('2013-04',52)
insert into @mTxt values ('2013-05',53)
insert into @mTxt values ('2013-06',54)
insert into @mTxt values ('2013-07',55)
insert into @mTxt values ('2013-08',56)
insert into @mTxt values ('2013-09',57)
insert into @mTxt values ('2013-10',58)
insert into @mTxt values ('2013-11',59)
insert into @mTxt values ('2013-12',60)
insert into @mTxt values ('2014-01',61)
insert into @mTxt values ('2014-02',62)
insert into @mTxt values ('2014-03',63)
insert into @mTxt values ('2014-04',64)
insert into @mTxt values ('2014-05',65)
insert into @mTxt values ('2014-06',66)
insert into @mTxt values ('2014-07',67)
insert into @mTxt values ('2014-08',68)
insert into @mTxt values ('2014-09',69)
insert into @mTxt values ('2014-10',70)
insert into @mTxt values ('2014-11',71)
insert into @mTxt values ('2014-12',72)
insert into @mTxt values ('2015-01',73)
insert into @mTxt values ('2015-02',74)
insert into @mTxt values ('2015-03',75)
insert into @mTxt values ('2015-04',76)
insert into @mTxt values ('2015-05',77)
insert into @mTxt values ('2015-06',78)
insert into @mTxt values ('2015-07',79)
insert into @mTxt values ('2015-08',80)
insert into @mTxt values ('2015-09',81)
insert into @mTxt values ('2015-10',82)
insert into @mTxt values ('2015-11',83)
insert into @mTxt values ('2015-12',84)
insert into @mTxt values ('2016-01',85)
insert into @mTxt values ('2016-02',86)
insert into @mTxt values ('2016-03',87)
insert into @mTxt values ('2016-04',88)
insert into @mTxt values ('2016-05',89)
insert into @mTxt values ('2016-06',90)
insert into @mTxt values ('2016-07',91)
insert into @mTxt values ('2016-08',92)
insert into @mTxt values ('2016-09',93)
insert into @mTxt values ('2016-10',94)
insert into @mTxt values ('2016-11',95)
insert into @mTxt values ('2016-12',96)


DECLARE @Itr        INT = 1,
        @RowCnt     INT=(SELECT COUNT(*) FROM @mTxt AS mt)


DECLARE @YellowColumnsBefore_2015 VARCHAR(MAX)='
	   vendor_id,
       tpep_pickup_datetime,
       tpep_dropoff_datetime,
       passenger_count,
       trip_distance,
       pickup_longitude,
       pickup_latitude,
       rate_code_id,
       store_and_fwd_flag,
       dropoff_longitude,
       dropoff_latitude,
       payment_type,
       fare_amount,       
       mta_tax,
       tip_amount,
       tolls_amount,
       total_amount'
		
DECLARE @YellowColumns_2015_2016_B4_07 VARCHAR(MAX)='
	   vendor_id,
       tpep_pickup_datetime,
       tpep_dropoff_datetime,
       passenger_count,
       trip_distance,
       pickup_longitude,
       pickup_latitude,
       rate_code_id,
       store_and_fwd_flag,
       dropoff_longitude,
       dropoff_latitude,
       payment_type,
       fare_amount,
       extra,
       mta_tax,
       tip_amount,
       tolls_amount,
       improvement_surcharge,
       total_amount'

	   DECLARE @YellowColumnsAfter_2016_07 VARCHAR(MAX)='
	   vendor_id,
       tpep_pickup_datetime,
       tpep_dropoff_datetime,
       passenger_count,
       trip_distance,
       rate_code_id,
       store_and_fwd_flag,
       pickup_location_id,
       dropoff_location_id,
       payment_type,
       fare_amount,
       extra,
       mta_tax,
       tip_amount,
       tolls_amount,
       improvement_surcharge,
       total_amount'



/*******************************************
 * Creating Files and FileGroup
 *******************************************/
 DECLARE @SQL VARCHAR(MAX)=''
 DECLARE @tmpFileVal VARCHAR(40)
 DECLARE @FormatFile VARCHAR(100)
 DECLARE @SQLForAsyncRun VARCHAR(MAX)='
		 BEGIN TRY
			UPDATE TripInsertStmnt
			SET    StartTime     = GETDATE()
			WHERE  FILENAME      = ''!!FileName'';
	
			!!!ActualSQL 
			
			ALTER TABLE [!!StgTableName] WITH CHECK ADD CONSTRAINT check_!!StgTableName
			CHECK ([tpep_pickup_datetime] >= ''!!StartDate'' AND [tpep_pickup_datetime] <= ''!!EndDate 23:59:59.000'');
			
			UPDATE TripInsertStmnt
			SET    EndTime     = GETDATE()
			WHERE  FILENAME      = ''!!FileName'';
	
		END TRY
		BEGIN CATCH
			UPDATE TripInsertStmnt
			SET	
				ErrorMessage = ERROR_MESSAGE(),
				EndTime = GETDATE()
				WHERE  FILENAME      = ''!!FileName'';
		END CATCH'
 
 WHILE (@Itr<=@RowCnt)
 BEGIN
 	
 	DECLARE 
 		@TmpCSVFilePath  AS VARCHAR(100) = @CSVFilePath,
 		@TmpNDFFileName  AS VARCHAR(100) = @NDFFileName,
 		@TmpFileGroup    AS VARCHAR(100) = @FileGroup,
 		@TmpNDFIlePath   AS VARCHAR(100) = @NDFIlePath,
 		@TmpStgTableName AS VARCHAR(100) = @StgTableName,
 		@TmpSQLForAsyncRun AS VARCHAR(MAX)=@SQLForAsyncRun,
 		@StartDate VARCHAR(20),
 		@EndDate   VARCHAR(20)
 		
 	
	SELECT @tmpFileVal=t.txt FROM @mTxt t WHERE t.id=@Itr
	
	SET @StartDate= @tmpFileVal + '-' + '01'
	SET @EndDate = CAST(EOMONTH(@StartDate) AS VARCHAR(20))
	--Need Raw filename here
	SET @TmpCSVFilePath=REPLACE(@CSVFilePath,'~',@tmpFileVal)
	-- Now setting _ insted of -
	SET @tmpFileVal=REPLACE(@tmpFileVal,'-','_')
	
	SET @TmpNDFFileName=REPLACE(@NDFFileName,'~',@tmpFileVal);
	SET @TmpFileGroup=REPLACE(@FileGroup,'~',@tmpFileVal);
	SET @TmpNDFIlePath=REPLACE(@NDFIlePath,'~',@tmpFileVal);
	SET @TmpStgTableName = REPLACE(@StgTableName, '~', @tmpFileVal);
	
	
	--File Group
	SET @SQL=REPLACE(REPLACE( @FGSQL,'!!FileGroup',@TmpFileGroup),'!!NDFFileName',@TmpNDFFileName)
	EXEC(@SQL)
	
	--NDF File
	SET @SQL= REPLACE(REPLACE(REPLACE(@NDFSQL,'!!NDFFileName',@TmpNDFFileName),'!!NDFIlePath',@TmpNDFIlePath),'!!FileGroup',@TmpFileGroup)
	--PRINT @SQL
	EXEC(@SQL)
	
	-- Staging Table

	SET @SQL= REPLACE(REPLACE(@tblTxt,'!!StgTableName',@TmpStgTableName),'!!FileGroup',@TmpFileGroup)
	EXEC(@SQL)
	
	IF LEFT(@tmpFileVal, 4) < 2015  
		BEGIN
			SET @ColumnList=@YellowColumnsBefore_2015
			SET @FormatFile=@FormatBefore_2015
		END
	ELSE IF LEFT(@tmpFileVal, 4) = 2015 OR (LEFT(@tmpFileVal, 4) = 2016 AND CAST(RIGHT(@tmpFileVal, 2) AS INT) < 7 )
		BEGIN
			SET @ColumnList=@YellowColumns_2015_2016_B4_07
			SET @FormatFile=@Format2015_2016_B4_07
		END
	ELSE
		BEGIN
			SET @ColumnList=@YellowColumnsAfter_2016_07
			SET @FormatFile=@FormatAfter_2016_07
		END
	
	SET @SQL=  REPLACE(REPLACE(@BulkInsert,'!!StgTableName',@TmpStgTableName),'!!ColumnList',@ColumnList)
	SET @SQL= REPLACE(REPLACE(@SQL,'!!CSVFilePath', @TmpCSVFilePath),'!!FormatFile',@FormatFile)
	
	
	SET @TmpSQLForAsyncRun= REPLACE(@TmpSQLForAsyncRun,'!!FileName',@DataKey + @tmpFileVal)
	--PRINT @SQLForAsyncRun
	
	SET @TmpSQLForAsyncRun=REPLACE(@TmpSQLForAsyncRun,'!!!ActualSQL',@SQL)
	
	-- For Check Constraint
	SET @TmpSQLForAsyncRun=REPLACE(@TmpSQLForAsyncRun,'!!StgTableName',@TmpStgTableName)
	SET @TmpSQLForAsyncRun=REPLACE(REPLACE(@TmpSQLForAsyncRun,'!!StartDate',@StartDate),'!!EndDate',@EndDate)
	--PRINT @TmpSQLForAsyncRun
	
	
	DELETE FROM TripInsertStmnt WHERE [FileName] = @DataKey + @tmpFileVal
	INSERT INTO TripInsertStmnt	([FileName],[Query]	) VALUES (@DataKey + @tmpFileVal , @TmpSQLForAsyncRun)
	
	SET @Itr=@Itr +1		
 END


