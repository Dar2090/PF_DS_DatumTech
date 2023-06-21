
COPY INTO dbo.reviews_google
(user_id 1, review_id 6, rating 3, gmap_id 2, date 5)
FROM 'https://datumlake.dfs.core.windows.net/datumtech/gold/GoogleMapsgold/reviews-estados-gold'
WITH
(
	FILE_TYPE = 'PARQUET'
	,MAXERRORS = 0
	,COMPRESSION = 'snappy'
)
GO

SELECT TOP 10 * FROM dbo.reviews_google
GO

SELECT COUNT(*) AS TOTAL FROM dbo.reviews_google
GO