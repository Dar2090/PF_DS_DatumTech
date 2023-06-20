
COPY INTO dbo.sentiment_google
(review_id 1, sentiment_score 2, sentiment 3)
FROM 'https://datumlake.dfs.core.windows.net/datumtech/gold/GoogleMapsgold/sentiment-gold'
WITH
(
	FILE_TYPE = 'PARQUET'
	,MAXERRORS = 0
	,COMPRESSION = 'snappy'
)
GO

SELECT TOP 100 * FROM dbo.sentiment_google
ORDER BY sentiment_score
GO

SELECT COUNT(*) AS TOTAL FROM dbo.sentiment_google
GO