
COPY INTO dbo.sentiment_yelp
(review_id 1, sentiment_score 2, sentiment 3)
FROM 'https://datumlake.dfs.core.windows.net/datumtech/gold/Yelpgold/sentiment-gold'
WITH
(
	FILE_TYPE = 'PARQUET'
	,MAXERRORS = 0
	,COMPRESSION = 'snappy'
)
GO

SELECT TOP 10 * FROM dbo.sentiment_yelp
ORDER BY sentiment_score DESC
GO


SELECT COUNT(*) AS TOTAL FROM dbo.sentiment_yelp
GO