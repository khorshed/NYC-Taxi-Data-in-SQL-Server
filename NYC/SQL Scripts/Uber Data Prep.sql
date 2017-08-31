SET NOCOUNT ON



DECLARE  @mTxt TABLE (txt VARCHAR(100) NOT NULL, id INT NOT NULL)
insert into @mTxt values ('Uber_apr14',1)
insert into @mTxt values ('Uber_may14',2)
insert into @mTxt values ('Uber_jun14',3)
insert into @mTxt values ('Uber_jul14',4)
insert into @mTxt values ('Uber_aug14',5)
insert into @mTxt values ('Uber_sep14',6)
insert into @mTxt values ('Uber_janjune_15',7)





SELECT  REPLACE(
'ALTER DATABASE TaxiData
ADD FILEGROUP ~
GO','~','FG_' + txt) AS FG
FROM @mTxt AS mt


SELECT  REPLACE(
'ALTER DATABASE TaxiData
    ADD FILE (
    NAME = [P_~],
    FILENAME = ''C:\TaxiData\Uber\~.ndf'',
        SIZE = 1024 KB, 
        MAXSIZE = UNLIMITED, 
        FILEGROWTH = 1024 MB
    ) TO FILEGROUP [FG_' + mt.txt + ']','~',mt.txt)

FROM @mTxt AS mt



