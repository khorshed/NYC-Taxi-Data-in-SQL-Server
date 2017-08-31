SET NOCOUNT ON



DECLARE  @mTxt TABLE (txt VARCHAR(MAX) NOT NULL, id INT NOT NULL)
insert into @mTxt values ('FHV_2015_01',1)
insert into @mTxt values ('FHV_2015_02',2)
insert into @mTxt values ('FHV_2015_03',3)
insert into @mTxt values ('FHV_2015_04',4)
insert into @mTxt values ('FHV_2015_05',5)
insert into @mTxt values ('FHV_2015_06',6)
insert into @mTxt values ('FHV_2015_07',7)
insert into @mTxt values ('FHV_2015_08',8)
insert into @mTxt values ('FHV_2015_09',9)
insert into @mTxt values ('FHV_2015_10',10)
insert into @mTxt values ('FHV_2015_11',11)
insert into @mTxt values ('FHV_2015_12',12)
insert into @mTxt values ('FHV_2016_01',13)
insert into @mTxt values ('FHV_2016_02',14)
insert into @mTxt values ('FHV_2016_03',15)
insert into @mTxt values ('FHV_2016_04',16)
insert into @mTxt values ('FHV_2016_05',17)
insert into @mTxt values ('FHV_2016_06',18)
insert into @mTxt values ('FHV_2016_07',19)
insert into @mTxt values ('FHV_2016_08',20)
insert into @mTxt values ('FHV_2016_09',21)
insert into @mTxt values ('FHV_2016_10',22)
insert into @mTxt values ('FHV_2016_11',23)
insert into @mTxt values ('FHV_2016_12',24)

/*
SELECT  REPLACE(
'ALTER DATABASE TaxiData
ADD FILEGROUP ~
GO','~','FG_' + txt) AS FG
FROM @mTxt AS mt


SELECT  REPLACE(
'ALTER DATABASE TaxiData
    ADD FILE 
    (
    NAME = [P_~],
    FILENAME = ''C:\TaxiData\FHV\~.ndf'',
        SIZE = 1024 KB, 
        MAXSIZE = UNLIMITED, 
        FILEGROWTH = 1024 MB
    ) TO FILEGROUP [FG_~]','~',mt.txt)

FROM @mTxt AS mt
*/

DECLARE @tblTxt VARCHAR(MAX)=''


SET @tblTxt='
IF OBJECT_ID(''green_tripdata_~'') IS NOT NULL
DROP TABLE green_tripdata_~
GO
CREATE TABLE green_tripdata_~ (  
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


