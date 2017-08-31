SET NOCOUNT ON
USE  TaxiData

GO

DECLARE @DataKey VARCHAR(10)='FHV'

DECLARE 
@CSVFilePath				VARCHAR(100)='F:\Big Data\CSV\' + @DataKey + '\fhv_tripdata_~.csv',
@FileGroup					VARCHAR(100)='FG_' + @DataKey + '~',
@NDFIlePath					VARCHAR(100) ='C:\TaxiData\'+ @dataKey+'\~.ndf',
@NDFFileName				VARCHAR(100)='P_' + @DataKey + '~'

DECLARE @FormatDefaut		VARCHAR(1000)='F:\Big Data\FormatFile\fhv.xml'

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
	Dispatching_base_num	VARCHAR(20),
	Pickup_date				DATETIME2,
	locationID				DATETIME2
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

insert into @mTxt values ('2015-01',1)
insert into @mTxt values ('2015-02',2)
insert into @mTxt values ('2015-03',3)
insert into @mTxt values ('2015-04',4)
insert into @mTxt values ('2015-05',5)
insert into @mTxt values ('2015-06',6)
insert into @mTxt values ('2015-07',7)
insert into @mTxt values ('2015-08',8)
insert into @mTxt values ('2015-09',9)
insert into @mTxt values ('2015-10',10)
insert into @mTxt values ('2015-11',11)
insert into @mTxt values ('2015-12',12)
insert into @mTxt values ('2016-01',13)
insert into @mTxt values ('2016-02',14)
insert into @mTxt values ('2016-03',15)
insert into @mTxt values ('2016-04',16)
insert into @mTxt values ('2016-05',17)
insert into @mTxt values ('2016-06',18)
insert into @mTxt values ('2016-07',19)
insert into @mTxt values ('2016-08',20)
insert into @mTxt values ('2016-09',21)
insert into @mTxt values ('2016-10',22)
insert into @mTxt values ('2016-11',23)
insert into @mTxt values ('2016-12',24)



DECLARE @Itr        INT = 1,
        @RowCnt     INT=(SELECT COUNT(*) FROM @mTxt AS mt)


DECLARE @ColumnsDefault VARCHAR(MAX)='
	Dispatching_base_num,
	Pickup_date,
	locationID'
		
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
	--PRINT @SQL
	--EXEC(@SQL)
	
	--NDF File
	SET @SQL= REPLACE(REPLACE(REPLACE(@NDFSQL,'!!NDFFileName',@TmpNDFFileName),'!!NDFIlePath',@TmpNDFIlePath),'!!FileGroup',@TmpFileGroup)
	--PRINT @SQL
	--EXEC(@SQL)
	
	-- Staging Table

	SET @SQL= REPLACE(REPLACE(@tblTxt,'!!StgTableName',@TmpStgTableName),'!!FileGroup',@TmpFileGroup)
	--PRINT @SQL
	--EXEC(@SQL)
	
		
	SET @ColumnList=@ColumnsDefault
	SET @FormatFile=@FormatDefaut
		
	
	
	SET @SQL=  REPLACE(REPLACE(@BulkInsert,'!!StgTableName',@TmpStgTableName),'!!ColumnList',@ColumnList)
	SET @SQL= REPLACE(REPLACE(@SQL,'!!CSVFilePath', @TmpCSVFilePath),'!!FormatFile',@FormatFile)
	
	
	SET @TmpSQLForAsyncRun= REPLACE(@TmpSQLForAsyncRun,'!!FileName',@DataKey + @tmpFileVal)
	--PRINT @SQLForAsyncRun
	
	SET @TmpSQLForAsyncRun=REPLACE(@TmpSQLForAsyncRun,'!!!ActualSQL',@SQL)
	
	-- For Check Constraint
	SET @TmpSQLForAsyncRun=REPLACE(@TmpSQLForAsyncRun,'!!StgTableName',@TmpStgTableName)
	SET @TmpSQLForAsyncRun=REPLACE(REPLACE(@TmpSQLForAsyncRun,'!!StartDate',@StartDate),'!!EndDate',@EndDate)
	
	--DELETE FROM TripInsertStmnt WHERE [FileName] = @DataKey + @tmpFileVal
	--INSERT INTO TripInsertStmnt	([FileName],[Query]	) VALUES (@DataKey + @tmpFileVal , @TmpSQLForAsyncRun)
	
	SET @Itr=@Itr +1		
 END


