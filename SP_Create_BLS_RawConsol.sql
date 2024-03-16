USE [TestingSP]
GO
/****** Object:  StoredProcedure [dbo].[SP_Create_Tbl_BLS_Data]    Script Date: 16-03-2024 00:13:12 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER   PROCEDURE [dbo].[SP_Create_Tbl_BLS_Data] AS 
IF OBJECT_ID('dbo.Tbl_BLS_Data') IS NOT NULL 
DROP TABLE dbo.Tbl_BLS_Data;
IF OBJECT_ID('dbo.demoTable') IS NOT NULL 
DROP TABLE dbo.demoTable;
IF OBJECT_ID('dbo.demo2Table') IS NOT NULL 
DROP TABLE dbo.demo2Table;


DECLARE @cols AS NVARCHAR(MAX) 
DECLARE @query AS NVARCHAR(MAX)
DECLARE @query2 as nvarchar(max)
DECLARE @mainquery as nvarchar(max)
declare @n int
declare @column1 varchar(100), @column2 varchar(100)
declare @counter int
set @counter = 1
declare @Flag int;




WITH TempColumns AS (
SELECT 
-- Fixed
BLSRC.[AREA]
, BLSRC.[AREA_TITLE]
, BLSRC.[AREA_TYPE]
, BLSRC.[PRIM_STATE]
, BLSRC.[NAICS]
, BLSRC.[NAICS_TITLE]
, BLSRC.[I_GROUP]
, BLSRC.[OWN_CODE]
, COALESCE(WTHist.OCC_CODE_2020,BLSRC.[OCC_CODE]) AS [OCC_CODE]
, COALESCE(WTHist.OCC_TITLE_2020,BLSRC.[OCC_TITLE]) AS [OCC_TITLE]
, BLSRC.[O_GROUP]
--BLSRC. Unpivot
, BLSRC.[TOT_EMP]
, BLSRC.[EMP_PRSE]
, BLSRC.[JOBS_1000]
, BLSRC.[LOC_QUOTIENT]
, BLSRC.[H_MEAN]
, BLSRC.[A_MEAN]
, BLSRC.[H_PCT10]
, BLSRC.[H_PCT25]
, BLSRC.[H_MEDIAN]
, BLSRC.[H_PCT75]
, BLSRC.[H_PCT90]
, BLSRC.[A_PCT10]
, BLSRC.[A_PCT25]
, BLSRC.[A_MEDIAN]
, BLSRC.[A_PCT75]
, BLSRC.[A_PCT90]
, BLSRC.BLSYr
, BLSRC.Src
FROM Tbl_BLS_RawConsol BLSRC
LEFT OUTER JOIN Tbl_BLS_WorkerTypeHistLookup WTHist
ON BLSRC.OCC_CODE = WTHist.OCC_CODE_2018
	AND BLSRC.OCC_TITLE = WTHist.OCC_TITLE_2018
)
, TempColumnsUnpivot AS (
SELECT [AREA]
, [AREA_TITLE]
, [AREA_TYPE]
, CASE
	WHEN CHARINDEX('-',LEFT(AREA_TITLE,CHARINDEX(',',AREA_TITLE)-1)) > 0 
	THEN LEFT(LEFT(AREA_TITLE,CHARINDEX(',',AREA_TITLE)-1),CHARINDEX('-',LEFT(AREA_TITLE,CHARINDEX(',',AREA_TITLE)-1))-1)
	ELSE LEFT(AREA_TITLE,CHARINDEX(',',AREA_TITLE)-1)
	END AS City
, [PRIM_STATE] AS [State]
, CASE
	WHEN CHARINDEX('-',LEFT(AREA_TITLE,CHARINDEX(',',AREA_TITLE)-1)) > 0 
	THEN LEFT(LEFT(AREA_TITLE,CHARINDEX(',',AREA_TITLE)-1),CHARINDEX('-',LEFT(AREA_TITLE,CHARINDEX(',',AREA_TITLE)-1))-1) + ', ' + PRIM_STATE
	ELSE LEFT(AREA_TITLE,CHARINDEX(',',AREA_TITLE)-1) + ', ' + PRIM_STATE 
	END AS [CityState]
--, [NAICS]
--, [NAICS_TITLE]
--, [I_GROUP]
--, [OWN_CODE]
, [OCC_CODE]	AS WorkerTypeCode
, [OCC_TITLE]	AS WorkerType
, [O_GROUP]
, BLSYr
, Src
, FieldName
, CASE 
	WHEN FieldValue = '*' THEN NULL
	WHEN FieldValue = '**' THEN NULL
	WHEN FieldValue = '**' THEN NULL
	WHEN FieldValue = '#' THEN NULL --9999999999.99
	ELSE CAST(FieldValue AS DECIMAL(20,2)) 
	END AS FieldValue
FROM TempColumns
UNPIVOT (FieldValue
			FOR FieldName IN (
[TOT_EMP]
, [JOBS_1000]
, [LOC_QUOTIENT]
, [H_MEAN]
, [A_MEAN]
, [H_PCT10]
, [H_PCT25]
, [H_MEDIAN]
, [H_PCT75]
, [H_PCT90]
, [A_PCT10]
, [A_PCT25]
, [A_MEDIAN]
, [A_PCT75]
, [A_PCT90]
)) Unpvt
)
, TempColumnsUnpivotPvt AS (
SELECT [AREA],[AREA_TITLE],[AREA_TYPE],[City],[State],[CityState],[WorkerTypeCode],[WorkerType],[O_GROUP]
,[A_MEAN],[A_MEDIAN],[A_PCT10],[A_PCT25],[A_PCT75],[A_PCT90],[H_MEAN],[H_MEDIAN],[H_PCT10],[H_PCT25],[H_PCT75],[H_PCT90],[JOBS_1000],[LOC_QUOTIENT],[TOT_EMP]
, BLSYr
, Src
  FROM TempColumnsUnpivot
PIVOT(MAX(FieldValue)
	FOR FieldName IN ([A_MEAN],[A_MEDIAN],[A_PCT10],[A_PCT25],[A_PCT75],[A_PCT90],[H_MEAN],[H_MEDIAN],[H_PCT10],[H_PCT25],[H_PCT75],[H_PCT90],[JOBS_1000],[LOC_QUOTIENT],[TOT_EMP])
) Pvt
)
select * into demoTable from TempColumnsUnpivotPvt;

-- Calc % Diff -- Part 1 

SELECT @cols = STUFF((SELECT DISTINCT ',' + QUOTENAME(BLSYr)
                      FROM [dbo].[demoTable]
                      FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'),1,1,'')
 
select @column1 = [Rank] from (select max(BLSYr) as [Rank] from dbo.demoTable) as T
select @n = max([Rank]) from (select BLSYr, Rank() over (order by BLSYr)  as [Rank] from dbo.demoTable group by BLSYr) as T
--print @n
set @query2 = ''
set @query2 = @query2 + 'SELECT AREA, AREA_TITLE, WorkerTypeCode, WorkerType, ' + @cols;

while @counter < @n
begin
    set @Flag = @n-@counter
	select @column2 = BLSYr from (select BLSYr, Rank() over (Order by BLSYr) as [Rank] from dbo.demoTable group by BLSYr) as T where [Rank] = @counter
	set @query2 = @query2 + ' , POWER(['+ @column1 + ']/[' + @column2 + '],(1/(cast('+ @column1+' as float)-cast('+@column2 +' as float)))) -1  as CAGR_'+@column2 +'_'+@column1+''
	set @counter = @counter + 1
end

SET @query =@query2 +' into demo2table FROM (
    SELECT AREA, AREA_TITLE, WorkerTypeCode, WorkerType, BLSYr, H_MEDIAN as H_MEDIAN
	FROM demoTable) as src
PIVOT (
MAX(H_MEDIAN) FOR BLSYr IN (' + @cols + ')
) AS pivoted_data'

exec sp_executesql @query

SELECT Unpvt.*
	, CAGR_2019_2020
	, CAGR_2018_2020
	, CAGR_2017_2020
	, CAGR_2016_2020
INTO dbo.Tbl_BLS_Data
FROM demoTable Unpvt
LEFT OUTER JOIN demo2table CPC
ON Unpvt.AREA = CPC.AREA
	AND Unpvt.AREA_TITLE = CPC.AREA_TITLE
	AND Unpvt.WorkerTypeCode = CPC.WorkerTypeCode
	AND Unpvt.WorkerType = CPC.WorkerType
WHERE O_GROUP = 'Detailed'
;