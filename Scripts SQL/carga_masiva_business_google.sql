
COPY INTO dbo.business_google
(gmap_id 3, address 1, name 6, latitude 4, longitude 5, category 9, review_count 7, avg_rating 2, url 8, state 10)
FROM 'https://datumlake.dfs.core.windows.net/datumtech/gold/GoogleMapsgold/metadata-sitios-gold/metadata-gold'
WITH
(
	FILE_TYPE = 'PARQUET'
	,MAXERRORS = 0
	,COMPRESSION = 'snappy'
)
GO

SELECT TOP 10 * FROM dbo.business_google
GO

SELECT COUNT(*) AS TOTAL FROM dbo.business_google
GO