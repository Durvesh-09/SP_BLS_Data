USE [TestingSP]
GO
/****** Object:  StoredProcedure [dbo].[SP_Create_Tbl_BLS_RawConsol]    Script Date: 12-03-2024 20:30:28 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE OR ALTER PROCEDURE [dbo].[SP_Create_Tbl_BLS_RawConsol] (@TableList NVARCHAR(MAX)) AS 
BEGIN
IF OBJECT_ID('dbo.Tbl_BLS_RawConsol') IS NOT NULL 
Truncate TABLE dbo.Tbl_BLS_RawConsol
 
DECLARE @TableName NVARCHAR(400)
DECLARE @columnname varchar (40)
DECLARE @columnname2 varchar (40)
DECLARE @DynSQL NVARCHAR(MAX)
DECLARE @Year INT
DECLARE @Src varchar(20)
DECLARE @SubstringIndex1 INT
DECLARE @SubstringIndex2 INT
SET @DynSQL = ''
DECLARE @DynSQ1L NVARCHAR(MAX)
SET @DynSQ1L = 'SELECT  [AREA],	[AREA_TITLE],	[AREA_TYPE], [PRIM_STATE],	[NAICS],	[NAICS_TITLE],	[I_GROUP],	[OWN_CODE],	[OCC_CODE],	[OCC_TITLE],	[O_GROUP],	[TOT_EMP],	[EMP_PRSE],	[JOBS_1000], [LOC_QUOTIENT], [PCT_TOTAL],	[H_MEAN],	[A_MEAN],	[MEAN_PRSE],	[H_PCT10],	[H_PCT25],	[H_MEDIAN],	[H_PCT75],	[H_PCT90],	[A_PCT10],	[A_PCT25],	[A_MEDIAN],	[A_PCT75],	[A_PCT90],	[ANNUAL],	[HOURLY],	[Source_APID], BLSYr, Src
INTO dbo.Tbl_BLS_RawConsol FROM ('
DECLARE @DynSQ2L NVARCHAR(MAX)
SET @DynSQ2L = ') t'
DECLARE @Counter INT
SET @Counter = 0
DECLARE @TableCount INT
SELECT @TableCount=COUNT(VALUE)
FROM  STRING_SPLIT(@TableList,',')
WHERE VALUE LIKE 'Tbl_%MSA_M20%'
 
WHILE @Counter < @TableCount
BEGIN
    SELECT @TableName = VALUE
    FROM (
        SELECT VALUE, RANK() OVER (ORDER BY VALUE desc) AS [Rank]
        FROM  STRING_SPLIT(@TableList,',')
        WHERE VALUE LIKE 'Tbl_%MSA_M20%'
    ) AS T
    WHERE [Rank] = @Counter + 1
	SELECT @columnname = COLUMN_NAME FROM (SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @TableName AND ORDINAL_POSITION = '3') as T
	SELECT @columnname2 = COLUMN_NAME FROM (SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @TableName AND ORDINAL_POSITION = '4') as T
	SET @SubstringIndex1 = CHARINDEX('_', @TableName)
	SET @SubstringIndex2 = CHARINDEX('2', @TableName)
	SET @Year = SUBSTRING(@TableName, @SubstringIndex2, 4) 
	SET @Year = cast(@Year as int)
	SELECT TRY_CAST(@Year as INT) as Result;
	SET @Src = SUBSTRING(@TableName, @SubstringIndex1+1, 17) 
	IF @columnname = 'AREA_TYPE' 
	 BEGIN
		 IF @columnname2 <> 'naics'
		 BEGIN
			SET @DynSQL =@DynSQL + 'select top 100 [AREA],	[AREA_TITLE],	[AREA_TYPE],	[PRIM_STATE],	[NAICS],	[NAICS_TITLE],	[I_GROUP],	[OWN_CODE],	[OCC_CODE],	[OCC_TITLE],	[O_GROUP],	[TOT_EMP],	[EMP_PRSE],	[JOBS_1000],	[LOC_QUOTIENT],	[PCT_TOTAL],	[H_MEAN],	[A_MEAN],	[MEAN_PRSE],	[H_PCT10],	[H_PCT25],	[H_MEDIAN],	[H_PCT75],	[H_PCT90],	[A_PCT10],	[A_PCT25],	[A_MEDIAN],	[A_PCT75],	[A_PCT90],	[ANNUAL],	[HOURLY],	[Source_APID], ' + @Year + ' AS BLSYr, '''+ @Src+ ''' AS Src FROM ' + @TableName
		 END
		 ELSE
		 BEGIN
			SET @DynSQL =@DynSQL + 'SELECT top 100 [area],	[area_title],	[area_type],	RIGHT(area_title,2),	[naics],	[naics_title],	[i_group],	[own_code],	[occ_code],	[occ_title],	[o_group],	[tot_emp],	[emp_prse],	[jobs_1000],	[loc_quotient],	[pct_total],	[h_mean],	[a_mean],	[mean_prse],	[h_pct10],	[h_pct25],	[h_median],	[h_pct75],	[h_pct90],	[a_pct10],	[a_pct25],	[a_median],	[a_pct75],	[a_pct90],	[annual],	[hourly],	[Source_APID], '+ @Year +' AS BLSYr, '''+ @Src+ ''' AS Src FROM ' + @TableName
		 END
	 END
     ELSE
	 BEGIN
		SET @DynSQL = @DynSQL + 'select top 100 [AREA], [AREA_NAME],	NULL,	[PRIM_STATE],	NULL,	NULL,	NULL,	NULL,	[OCC_CODE],	[OCC_TITLE],	[OCC_GROUP],	[TOT_EMP],	[EMP_PRSE],	[JOBS_1000],	[LOC QUOTIENT],	NULL,	[H_MEAN],	[A_MEAN],	[MEAN_PRSE],	[H_PCT10],	[H_PCT25],	[H_MEDIAN],	[H_PCT75],	[H_PCT90],	[A_PCT10],	[A_PCT25],	[A_MEDIAN],	[A_PCT75],	[A_PCT90],	[ANNUAL],	[HOURLY],	[Source_APID], ' + @Year + ' AS BLSYr, '''+ @Src+ ''' AS Src FROM ' + @TableName
     END
		IF @Counter < @TableCount - 1
    BEGIN
        SET @DynSQL = @DynSQL + ' UNION ALL '
    END
    SET @Counter = @Counter + 1
END
PRINT @DynSQ1L + @DynSQL + @DynSQ2L
 
DECLARE @MainSQL NVARCHAR(MAX)
SET @MainSQL = @DynSQ1L + @DynSQL + @DynSQ2L
EXEC sp_executesql @MainSQL
END
 

 
EXEC [dbo].[SP_Create_Tbl_BLS_RawConsol] 'Tbl_MSA_M2020_dl_xlsx_MSA_M2020_dl,Tbl_MSA_M2017_dl_xlsx_MSA_dl_1,Tbl_MSA_M2018_dl_xlsx_MSA_dl_1,Tbl_MSA_M2016_dl_xlsx_MSA_dl_1,Tbl_aMSA_M2016_dl_xlsx_aMSA_dl,Tbl_aMSA_M2017_dl_xlsx_aMSA_dl,Tbl_MSA_M2019_dl_xlsx'




select * from  dbo.Tbl_BLS_RawConsol