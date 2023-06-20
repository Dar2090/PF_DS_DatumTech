CREATE TABLE [dbo].[state]
( 
	[state_id] [int]  NOT NULL IDENTITY(1,1),
	[state] [nvarchar](10)  NOT NULL,
	[name] [nvarchar](1000)  NOT NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO

CREATE TABLE [dbo].[business_google]
(
    [gmap_id] NVARCHAR(1000) NOT NULL,
    [address] NVARCHAR(4000),
    [name] NVARCHAR(4000),
    [latitude] FLOAT NOT NULL,
    [longitude] FLOAT NOT NULL,
    [category] NVARCHAR(4000) NOT NULL,
    [review_count] INT,
    [avg_rating] FLOAT,
    [url] NVARCHAR(4000),
    [state] NVARCHAR(10) NOT NULL,
    CONSTRAINT PK_business_google PRIMARY KEY NONCLUSTERED ([gmap_id] ASC) NOT ENFORCED
)
WITH
(
    DISTRIBUTION = ROUND_ROBIN,
    CLUSTERED COLUMNSTORE INDEX
)
GO

CREATE TABLE [dbo].[reviews_google]
(
    [user_id] NVARCHAR(1000) NOT NULL,
    [review_id] NVARCHAR(4000) NOT NULL,
    [rating] INT NOT NULL,
    [gmap_id] NVARCHAR(1000) NOT NULL, 
    [date] DATE NOT NULL,
    CONSTRAINT PK_reviews_google PRIMARY KEY NONCLUSTERED ([review_id] ASC) NOT ENFORCED
)
WITH
(
    DISTRIBUTION = ROUND_ROBIN,
    CLUSTERED COLUMNSTORE INDEX
)
GO

CREATE TABLE [dbo].[business_yelp]
(
    [business_id] NVARCHAR(1000) NOT NULL,
    [name] NVARCHAR(4000),
    [address] NVARCHAR(4000),
    [latitude] FLOAT NOT NULL,
    [longitude] FLOAT NOT NULL,
    [state] NVARCHAR(10) NOT NULL,
    [category] NVARCHAR(4000) NOT NULL,
    [avg_rating] FLOAT,
    [review_count] INT,
    [attributes] NVARCHAR(4000),
    CONSTRAINT PK_business_yelp PRIMARY KEY NONCLUSTERED ([business_id] ASC) NOT ENFORCED
)

WITH
(
    DISTRIBUTION = ROUND_ROBIN,
    CLUSTERED COLUMNSTORE INDEX
)
GO

CREATE TABLE [dbo].[reviews_yelp]
(
    [user_id] NVARCHAR(1000) NOT NULL,
    [review_id] NVARCHAR(1000) NOT NULL,
    [business_id] NVARCHAR(1000) NOT NULL,
    [rating] INT,
    [date] DATE NOT NULL,
    CONSTRAINT PK_reviews_yelp PRIMARY KEY NONCLUSTERED ([review_id] ASC) NOT ENFORCED
)
WITH
(
    DISTRIBUTION = ROUND_ROBIN,
    CLUSTERED COLUMNSTORE INDEX
)
GO

CREATE TABLE [dbo].[sentiment_yelp]
(
    [review_id] NVARCHAR(4000) NOT NULL,
    [sentiment_score] FLOAT NOT NULL,
    [sentiment] NVARCHAR(4000) NOT NULL,
    CONSTRAINT PK_sentiment_yelp PRIMARY KEY NONCLUSTERED ([review_id] ASC) NOT ENFORCED
)
WITH
(
    DISTRIBUTION = ROUND_ROBIN,
    CLUSTERED COLUMNSTORE INDEX
)
GO

CREATE TABLE [dbo].[sentiment_google]
(
    [review_id] NVARCHAR(4000) NOT NULL,
    [sentiment_score] FLOAT NOT NULL,
    [sentiment] NVARCHAR(4000) NOT NULL,
    CONSTRAINT PK_sentiment_google PRIMARY KEY NONCLUSTERED ([review_id] ASC) NOT ENFORCED
)
WITH
(
    DISTRIBUTION = ROUND_ROBIN,
    CLUSTERED COLUMNSTORE INDEX
)
GO


