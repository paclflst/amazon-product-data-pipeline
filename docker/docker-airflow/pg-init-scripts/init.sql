CREATE USER test WITH PASSWORD 'postgres';
CREATE DATABASE test;
GRANT ALL PRIVILEGES ON DATABASE test TO test;

\c test;

CREATE TABLE public.meta_movies_and_tv (
	asin text NULL,
	categories text NULL,
	description text NULL,
	title text NULL,
	sales_rank text NULL,
	im_url text NULL,
	brand text NULL,
	related text NULL,
	price float4 NULL
);

CREATE TABLE public.ratings_movies_and_tv (
	"user" text NULL,
	item text NULL,
	rating float4 NULL,
	ts timestamp NULL
);

CREATE TABLE public.dm_ratings_by_month (
	"month" text NULL,
	item text NULL,
	title text NULL,
	brand text NULL,
	price float4 NULL,
	avg_rating float8 NULL
);

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO test;
ALTER TABLE public.dm_ratings_by_month OWNER TO test;

\c airflow;