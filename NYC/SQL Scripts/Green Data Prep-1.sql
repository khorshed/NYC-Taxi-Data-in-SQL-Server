SET NOCOUNT ON



DECLARE  @mTxt TABLE (txt VARCHAR(100) NOT NULL, id INT NOT NULL)
insert into @mTxt values ('2013_08',1)
insert into @mTxt values ('2013_09',2)
insert into @mTxt values ('2013_10',3)
insert into @mTxt values ('2013_11',4)
insert into @mTxt values ('2013_12',5)
insert into @mTxt values ('2014_01',6)
insert into @mTxt values ('2014_02',7)
insert into @mTxt values ('2014_03',8)
insert into @mTxt values ('2014_04',9)
insert into @mTxt values ('2014_05',10)
insert into @mTxt values ('2014_06',11)
insert into @mTxt values ('2014_07',12)
insert into @mTxt values ('2014_08',13)
insert into @mTxt values ('2014_09',14)
insert into @mTxt values ('2014_10',15)
insert into @mTxt values ('2014_11',16)
insert into @mTxt values ('2014_12',17)
insert into @mTxt values ('2015_01',18)
insert into @mTxt values ('2015_02',19)
insert into @mTxt values ('2015_03',20)
insert into @mTxt values ('2015_04',21)
insert into @mTxt values ('2015_05',22)
insert into @mTxt values ('2015_06',23)
insert into @mTxt values ('2015_07',24)
insert into @mTxt values ('2015_08',25)
insert into @mTxt values ('2015_09',26)
insert into @mTxt values ('2015_10',27)
insert into @mTxt values ('2015_11',28)
insert into @mTxt values ('2015_12',29)
insert into @mTxt values ('2016_01',30)
insert into @mTxt values ('2016_02',31)
insert into @mTxt values ('2016_03',32)
insert into @mTxt values ('2016_04',33)
insert into @mTxt values ('2016_05',34)
insert into @mTxt values ('2016_06',35)
insert into @mTxt values ('2016_07',36)
insert into @mTxt values ('2016_08',37)
insert into @mTxt values ('2016_09',38)
insert into @mTxt values ('2016_10',39)
insert into @mTxt values ('2016_11',40)
insert into @mTxt values ('2016_12',41)

/*

SELECT  REPLACE(
'ALTER DATABASE TaxiData
ADD FILEGROUP ~
GO','~','FG_' + txt) AS FG
FROM @mTxt AS mt


SELECT  REPLACE(
'ALTER DATABASE TaxiData
    ADD FILE (
    NAME = [P_~],
    FILENAME = ''C:\TaxiData\Green\~.ndf'',
        SIZE = 1024 KB, 
        MAXSIZE = UNLIMITED, 
        FILEGROWTH = 1024 MB
    ) TO FILEGROUP [FG_' + mt.txt + ']','~',mt.txt)

FROM @mTxt AS mt



DECLARE @tblTxt VARCHAR(MAX)=''


SET @tblTxt='
IF OBJECT_ID(''tripdata_~'') IS NOT NULL
DROP TABLE tripdata_~
GO
CREATE TABLE tripdata_~ (  
  vendor_id VARCHAR(4),
  lpep_pickup_datetime DATETIME2,
  lpep_dropoff_datetime DATETIME2,
  store_and_fwd_flag CHAR(1),
  rate_code_id VARCHAR(10),
  pickup_longitude numeric,
  pickup_latitude numeric,
  dropoff_longitude numeric,
  dropoff_latitude numeric,
  passenger_count int,
  trip_distance FLOAT,
  fare_amount FLOAT,
  extra FLOAT,
  mta_tax FLOAT,
  tip_amount FLOAT,
  tolls_amount FLOAT,
  ehail_fee FLOAT,
  improvement_surcharge FLOAT,
  total_amount FLOAT,
  payment_type INT,
  trip_type VARCHAR(20),
  pickup_location_id VARCHAR(20),
  dropoff_location_id VARCHAR(20),
  junk1 VARCHAR(50),
  junk2 VARCHAR(50)
) ON [FG_~]
GO
'

SELECT CAST( REPLACE(@tblTxt,'~',mt.txt) AS VARCHAR(MAX)) AS txt
FROM   @mTxt AS mt 
*/


DECLARE @insert VARCHAR(MAX)=''
SET @insert='
INSERT INTO tripdata_@TBL (@Q)
SELECT @Q
FROM   OPENROWSET(
           BULK ''D:\Big Data\CSV\Green\tripdata_~.csv'',
           FORMATFILE=''D:\Big Data\@FMT.xml'',
           FIRSTROW=2           
       ) AS DATA;'

DECLARE @Before201501 VARCHAR(MAX)='vendor_id,lpep_pickup_datetime,lpep_dropoff_datetime,store_and_fwd_flag,ratecodeid,pickup_longitude,pickup_latitude,dropoff_longitude,dropoff_latitude,
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
trip_type '

DECLARE @BETWEEN_201501_201606 VARCHAR(MAX)='
vendor_id,
lpep_pickup_datetime,
lpep_dropoff_datetime,
store_and_fwd_flag,
ratecodeid,
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
trip_type
 '

DECLARE @GT_201606 VARCHAR(MAX)='vendor_id,
lpep_pickup_datetime,
lpep_dropoff_datetime,
store_and_fwd_flag,
ratecodeid,
PULocationID,
DOLocationID,
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


SELECT 

CASE 
WHEN REPLACE(REPLACE(mt.txt,'',''),'_','') <201501 THEN		
		REPLACE(REPLACE(REPLACE(REPLACE(@insert,'~',REPLACE(replace(mt.txt,'',''),'_','-')),'@FMT','befor_2015'),'@Q',@Before201501),'@TBL',mt.txt) 

WHEN REPLACE(REPLACE(mt.txt,'',''),'_','') >=201501 AND REPLACE(REPLACE(mt.txt,'',''),'_','') <=201606 THEN
		REPLACE(REPLACE(REPLACE(REPLACE(@insert,'~',REPLACE(replace(mt.txt,'',''),'_','-')),'@FMT','2015_to_2016_06'),'@Q',@BETWEEN_201501_201606) ,'@TBL',mt.txt) 

WHEN REPLACE(REPLACE(mt.txt,'',''),'_','') >=201606 THEN
	  REPLACE(REPLACE( REPLACE(REPLACE(@insert,'~',REPLACE(replace(mt.txt,'',''),'_','-')),'@FMT','2016_07_to_end'),'@Q' ,@GT_201606),'@TBL',mt.txt) 
END		
FROM   @mTxt AS mt       