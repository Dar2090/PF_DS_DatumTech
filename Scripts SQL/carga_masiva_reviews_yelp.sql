
COPY INTO dbo.reviews_yelp
(user_id 2, review_id 1, business_id 3, rating 4, date 6)
FROM 'https://datumlake.dfs.core.windows.net/datumtech/gold/Yelpgold/reviews-gold/review-gold'
WITH
(
	FILE_TYPE = 'PARQUET'
	,MAXERRORS = 0
	,COMPRESSION = 'snappy'
)
GO

SELECT TOP 10 * FROM dbo.reviews_yelp
GO

SELECT COUNT(*) AS TOTAL FROM dbo.reviews_yelp
GO