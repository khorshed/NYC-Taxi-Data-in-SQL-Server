SET NOCOUNT ON



DECLARE  @mTxt TABLE (txt VARCHAR(100) NOT NULL, id INT NOT NULL)
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



