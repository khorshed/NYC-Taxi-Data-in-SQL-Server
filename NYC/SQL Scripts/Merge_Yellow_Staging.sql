USE TaxiData
GO
SET NOCOUNT ON;

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


DECLARE @DataKey VARCHAR(10)='Yellow'
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
 --EXEC(@PartionFunc)
 PRINT @PartionFunc

 SET @PartitionScheme='
 CREATE PARTITION SCHEME !!PScheName
 AS PARTITION !!PFuncName
 TO ('+ @PartionFG +');';
 
 SET @PartitionScheme=REPLACE(@PartitionScheme,'!!PScheName',@PScheName)
 SET @PartitionScheme=REPLACE(@PartitionScheme,'!!PFuncName',@PFuncName)
 
 PRINT @PartitionScheme
 --EXEC (@PartitionScheme)
 
DECLARE @StgTableSQL VARCHAR(MAX) ='

CREATE TABLE [dbo].'+ @StgTblName +'(
	[vendor_id] [varchar](5) NULL,
	[tpep_pickup_datetime] [datetime] NOT NULL,
	[tpep_dropoff_datetime] [datetime] NULL,
	[passenger_count] [varchar](30) NULL,
	[trip_distance] [varchar](30) NULL,
	[pickup_longitude] [varchar](30) NULL,
	[pickup_latitude] [varchar](30) NULL,
	[rate_code_id] [varchar](20) NULL,
	[store_and_fwd_flag] [varchar](30) NULL,
	[dropoff_longitude] [varchar](30) NULL,
	[dropoff_latitude] [varchar](30) NULL,
	[payment_type] [varchar](20) NULL,
	[fare_amount] [varchar](30) NULL,
	[extra] [varchar](30) NULL,
	[mta_tax] [varchar](30) NULL,
	[tip_amount] [varchar](30) NULL,
	[tolls_amount] [varchar](30) NULL,
	[improvement_surcharge] [varchar](30) NULL,
	[total_amount] [varchar](30) NULL,
	[pickup_location_id] [varchar](20) NULL,
	[dropoff_location_id] [varchar](20) NULL
) ON ['+ @PScheName +'](tpep_pickup_datetime);'

PRINT @StgTableSQL
 
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
	
	PRINT @SQL
	SET @Itr=@Itr +1		
 END
 

     