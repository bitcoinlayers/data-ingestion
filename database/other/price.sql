-- public.price definition

-- Drop table

-- DROP TABLE public.price;

CREATE TABLE public.price (
	id serial4 NOT NULL,
	"date" date NULL,
	token_slug varchar(255) NULL,
	token_name varchar(255) NULL,
	price_usd numeric(18, 8) NULL,
	CONSTRAINT price_pkey PRIMARY KEY (id),
	CONSTRAINT unique_date_token_slug UNIQUE (date, token_slug)
);