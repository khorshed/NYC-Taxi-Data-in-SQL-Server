SET NOCOUNT ON
USE  TaxiData

GO

DECLARE @DataKey VARCHAR(10)='Green'

DECLARE 
@CSVFilePath				VARCHAR(100)='F:\Big Data\CSV\' + @DataKey + '\green_tripdata_~.csv',
@FileGroup					VARCHAR(100)='FG_' + @DataKey + '~',
@NDFIlePath					VARCHAR(100) ='C:\TaxiData\'+ @dataKey+'\~.ndf',
@NDFFileName				VARCHAR(100)='P_' + @DataKey + '~'

DECLARE @FormatBefore_2015		VARCHAR(1000)='F:\Big Data\FormatFile\green_befor_2015.xml'
DECLARE @Format2015_2016_B4_07	VARCHAR(1000)='F:\Big Data\FormatFile\green_2015_to_2016_06.xml'
DECLARE @FormatAfter_2016_07	VARCHAR(1000)='F:\Big Data\FormatFile\green_2016_07_to_end.xml'



DECLARE  @FGSQL				VARCHAR(MAX) ='
IF OBJECT_ID(''!!StgTableName'') IS NOT NULL
DROP TABLE !!StgTableName;

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

ALTER DATABASE TaxiData ADD FILEGROUP !!FileGroup ;',

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

CREATE TABLE !!StgTableName
(
	ID BIGINT DEFAULT NEXT VALUE FOR Trip_Records_Seq, 
	vendor_id                 VARCHAR(4),
	lpep_pickup_datetime      DATETIME NOT NULL,
	lpep_dropoff_datetime     DATETIME,
	store_and_fwd_flag        CHAR(1),
	rate_code_id              VARCHAR(10),
	pickup_longitude          NUMERIC,
	pickup_latitude           NUMERIC,
	dropoff_longitude         NUMERIC,
	dropoff_latitude          NUMERIC,
	passenger_count           INT,
	trip_distance             FLOAT,
	fare_amount               FLOAT,
	extra                     FLOAT,
	mta_tax                   FLOAT,
	tip_amount                FLOAT,
	tolls_amount              FLOAT,
	ehail_fee                 FLOAT,
	improvement_surcharge     FLOAT,
	total_amount              FLOAT,
	payment_type              INT,
	trip_type                 VARCHAR(20),
	pickup_location_id        VARCHAR(20),
	dropoff_location_id       VARCHAR(20),
	junk1                     VARCHAR(50),
	junk2                     VARCHAR(50)
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

insert into @mTxt values ('2013-08',1)
insert into @mTxt values ('2013-09',2)
insert into @mTxt values ('2013-10',3)
insert into @mTxt values ('2013-11',4)
insert into @mTxt values ('2013-12',5)
insert into @mTxt values ('2014-01',6)
insert into @mTxt values ('2014-02',7)
insert into @mTxt values ('2014-03',8)
insert into @mTxt values ('2014-04',9)
insert into @mTxt values ('2014-05',10)
insert into @mTxt values ('2014-06',11)
insert into @mTxt values ('2014-07',12)
insert into @mTxt values ('2014-08',13)
insert into @mTxt values ('2014-09',14)
insert into @mTxt values ('2014-10',15)
insert into @mTxt values ('2014-11',16)
insert into @mTxt values ('2014-12',17)
insert into @mTxt values ('2015-01',18)
insert into @mTxt values ('2015-02',19)
insert into @mTxt values ('2015-03',20)
insert into @mTxt values ('2015-04',21)
insert into @mTxt values ('2015-05',22)
insert into @mTxt values ('2015-06',23)
insert into @mTxt values ('2015-07',24)
insert into @mTxt values ('2015-08',25)
insert into @mTxt values ('2015-09',26)
insert into @mTxt values ('2015-10',27)
insert into @mTxt values ('2015-11',28)
insert into @mTxt values ('2015-12',29)
insert into @mTxt values ('2016-01',30)
insert into @mTxt values ('2016-02',31)
insert into @mTxt values ('2016-03',32)
insert into @mTxt values ('2016-04',33)
insert into @mTxt values ('2016-05',34)
insert into @mTxt values ('2016-06',35)
insert into @mTxt values ('2016-07',36)
insert into @mTxt values ('2016-08',37)
insert into @mTxt values ('2016-09',38)
insert into @mTxt values ('2016-10',39)
insert into @mTxt values ('2016-11',40)
insert into @mTxt values ('2016-12',41)


DECLARE @Itr        INT = 1,
        @RowCnt     INT=(SELECT COUNT(*) FROM @mTxt AS mt)


DECLARE @ColumnsBefore_2015 VARCHAR(MAX)='
	   vendor_id,
       lpep_pickup_datetime,
       lpep_dropoff_datetime,
       store_and_fwd_flag,
       rate_code_id,
       pickup_longitude,
       pickup_latitude,
       dropoff_longitude,
       dropoff_latitude,
       passenger_count,
       trip_distance,
       fare_amount,
       extra,
       mta_tax,
       tip_amount,
       tolls_amount,
       ehail_fee,
       total_amount,
       payment_type,
       trip_type'
		
DECLARE @Columns_2016_B4_07 VARCHAR(MAX)='
	vendor_id,
	lpep_pickup_datetime,
	lpep_dropoff_datetime,
	store_and_fwd_flag,
	rate_code_id,
	pickup_longitude,
	pickup_latitude,
	dropoff_longitude,
	dropoff_latitude,
	passenger_count,
	trip_distance,
	fare_amount,
	extra,
	mta_tax,
	tip_amount,
	tolls_amount,
	ehail_fee,
	improvement_surcharge,
	total_amount,
	payment_type,
	trip_type'

	   DECLARE @ColumnsAfter_2016_07 VARCHAR(MAX)='
	    vendor_id,
		lpep_pickup_datetime,
		lpep_dropoff_datetime,
		store_and_fwd_flag,
		rate_code_id,
		pickup_Location_ID,
		dropoff_Location_ID,
		passenger_count,
		trip_distance,
		fare_amount,
		extra,
		mta_tax,
		tip_amount,
		tolls_amount,
		ehail_fee,
		improvement_surcharge,
		total_amount,
		payment_type,
		trip_type'



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
			CHECK ([lpep_pickup_datetime] >= ''!!StartDate'' AND [lpep_pickup_datetime] <= ''!!EndDate 23:59:59.000'');
			
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
	SET @SQL=REPLACE(@SQL,'!!StgTableName',@TmpStgTableName)
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
			SET @ColumnList=@ColumnsBefore_2015
			SET @FormatFile=@FormatBefore_2015
		END
	ELSE IF LEFT(@tmpFileVal, 4) = 2015 OR (LEFT(@tmpFileVal, 4) = 2016 AND CAST(RIGHT(@tmpFileVal, 2) AS INT) < 7 )
		BEGIN
			SET @ColumnList=@Columns_2016_B4_07
			SET @FormatFile=@Format2015_2016_B4_07
		END
	ELSE
		BEGIN
			SET @ColumnList=@ColumnsAfter_2016_07
			SET @FormatFile=@FormatAfter_2016_07
		END
	
	
	
	SET @SQL=  REPLACE(REPLACE(@BulkInsert,'!!StgTableName',@TmpStgTableName),'!!ColumnList',@ColumnList)
	SET @SQL= REPLACE(REPLACE(@SQL,'!!CSVFilePath', @TmpCSVFilePath),'!!FormatFile',@FormatFile)
	
	
	
	SET @TmpSQLForAsyncRun= REPLACE(@TmpSQLForAsyncRun,'!!FileName',@DataKey + @tmpFileVal)
	SET @TmpSQLForAsyncRun=REPLACE(@TmpSQLForAsyncRun,'!!!ActualSQL',@SQL)
	
	-- For Check Constraint
	SET @TmpSQLForAsyncRun=REPLACE(@TmpSQLForAsyncRun,'!!StgTableName',@TmpStgTableName)
	SET @TmpSQLForAsyncRun=REPLACE(REPLACE(@TmpSQLForAsyncRun,'!!StartDate',@StartDate),'!!EndDate',@EndDate)
	--PRINT @TmpSQLForAsyncRun
	
	DELETE FROM TripInsertStmnt WHERE [FileName] = @DataKey + @tmpFileVal
	INSERT INTO TripInsertStmnt	([FileName],[Query]	) VALUES (@DataKey + @tmpFileVal , @TmpSQLForAsyncRun)
	
	SET @Itr=@Itr +1		
 END


