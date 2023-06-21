
COPY INTO dbo.business_yelp
(business_id 1, name 2, address 3, latitude 5, longitude 6, state 4, category 9, avg_rating 7, review_count 8, attributes 10)
FROM 'https://datumlake.dfs.core.windows.net/datumtech/gold/Yelpgold/business-gold'
WITH
(
	FILE_TYPE = 'PARQUET'
	,MAXERRORS = 0
	,COMPRESSION = 'snappy'
)
GO

SELECT TOP 10 * FROM dbo.business_yelp
GO

SELECT COUNT(*) AS TOTAL FROM dbo.business_yelp
GO