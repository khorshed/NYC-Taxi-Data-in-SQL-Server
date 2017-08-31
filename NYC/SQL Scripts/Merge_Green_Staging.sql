USE TaxiData
GO
SET NOCOUNT ON;

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


DECLARE @DataKey VARCHAR(10)='Green'
DECLARE @StgTableName VARCHAR(100)= @DataKey+'_tripdata_~'

DECLARE @PartionFunc         VARCHAR(MAX),
        @PartitionScheme     VARCHAR(MAX),
        @PartiotnRange       VARCHAR(MAX),
        @PartionFG           VARCHAR(MAX)
       


SELECT @PartiotnRange = COALESCE(@PartiotnRange + ',', '') + '''' + REPLACE(txt, '-', '') + '01'''
FROM   @mTxt
WHERE  id > 1
ORDER BY
       Id
       
SELECT @PartionFG = COALESCE(@PartionFG + ',', '') + 'FG_' + @DataKey + REPLACE(txt, '-', '_') 
FROM   @mTxt
ORDER BY
       Id       

 
 DECLARE @PFuncName VARCHAR(30)= @DataKey + 'PartitionFunc'
 DECLARE @PScheName VARCHAR(30)= @DataKey + 'PartitionSche'
 DECLARE @StgTblName VARCHAR(100)=@DataKey + '_tripdata_staging'
 
 SET @PartionFunc = '
 IF OBJECT_ID(''!!StgTblName'') IS NOT NULL
 DROP TABLE !!StgTblName;

 IF EXISTS(
	 SELECT *
	 FROM   sys.partition_schemes AS ps
	 WHERE  ps.name = ''!!PScheName''
 )
  DROP PARTITION SCHEME !!PScheName;
  
 IF EXISTS(
        SELECT *
        FROM   sys.partition_functions AS pf
        WHERE  pf.name =''!!PFuncName''
    )
     DROP PARTITION FUNCTION !!PFuncName;
	
	CREATE PARTITION FUNCTION !!PFuncName (datetime)
	AS RANGE RIGHT FOR VALUES ( ' + @PartiotnRange + ');'
 
 SET @PartionFunc=REPLACE(@PartionFunc,'!!PFuncName',@PFuncName)
 SET @PartionFunc=REPLACE(@PartionFunc,'!!PScheName',@pscheName)
 SET @PartionFunc=REPLACE(@PartionFunc,'!!StgTblName',@StgTblName)
 EXEC(@PartionFunc)
 PRINT @PartionFunc

 SET @PartitionScheme='
 CREATE PARTITION SCHEME !!PScheName
 AS PARTITION !!PFuncName
 TO ('+ @PartionFG +');';
 
 SET @PartitionScheme=REPLACE(@PartitionScheme,'!!PScheName',@PScheName)
 SET @PartitionScheme=REPLACE(@PartitionScheme,'!!PFuncName',@PFuncName)
 
 --PRINT @PartitionScheme
 EXEC (@PartitionScheme)
 
DECLARE @StgTableSQL VARCHAR(MAX) ='

CREATE TABLE [dbo].'+ @StgTblName +'(
	ID BIGINT,
	[vendor_id] [varchar](4) NULL,
	[lpep_pickup_datetime] [datetime] NOT NULL,
	[lpep_dropoff_datetime] [datetime] NULL,
	[store_and_fwd_flag] [char](1) NULL,
	[rate_code_id] [varchar](10) NULL,
	[pickup_longitude] [numeric](18, 0) NULL,
	[pickup_latitude] [numeric](18, 0) NULL,
	[dropoff_longitude] [numeric](18, 0) NULL,
	[dropoff_latitude] [numeric](18, 0) NULL,
	[passenger_count] [int] NULL,
	[trip_distance] [float] NULL,
	[fare_amount] [float] NULL,
	[extra] [float] NULL,
	[mta_tax] [float] NULL,
	[tip_amount] [float] NULL,
	[tolls_amount] [float] NULL,
	[ehail_fee] [float] NULL,
	[improvement_surcharge] [float] NULL,
	[total_amount] [float] NULL,
	[payment_type] [int] NULL,
	[trip_type] [varchar](20) NULL,
	[pickup_location_id] [varchar](20) NULL,
	[dropoff_location_id] [varchar](20) NULL,
	[junk1] [varchar](50) NULL,
	[junk2] [varchar](50) NULL
) ON ['+ @PScheName +'](lpep_pickup_datetime);'

EXEC( @StgTableSQL)
--PRINT @StgTableSQL
 
 DECLARE @Itr        INT = 1,
        @RowCnt     INT=(SELECT COUNT(*) FROM @mTxt AS mt)
  
 WHILE (@Itr<=@RowCnt)
 BEGIN
 	
 	DECLARE		
 		@TmpStgTableName AS VARCHAR(100) = @StgTableName,
 		@SQL			 AS VARCHAR(MAX),
 		@tmpFileVal		 AS VARCHAR(200)
 		
 	
	SELECT @tmpFileVal=t.txt FROM @mTxt t WHERE t.id=@Itr	
	SET @tmpFileVal=REPLACE(@tmpFileVal,'-','_')
	
	SET @TmpStgTableName = REPLACE(@StgTableName, '~', @tmpFileVal);
	
	SET @SQL='ALTER TABLE ' + @TmpStgTableName + ' SWITCH TO ['+ @StgTblName +'] PARTITION ' + CAST(@Itr AS VARCHAR(10))
	
	EXEC( @SQL)
	SET @Itr=@Itr +1		
 END
 

     