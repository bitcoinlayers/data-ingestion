-- public.networks definition

-- Drop table

-- DROP TABLE public.networks;

CREATE TABLE public.networks (
	slug varchar(255) NOT NULL,
	name varchar(255) NOT NULL,
	explorer varchar(255) NULL,
	is_bitcoin_layer bool NULL,
	created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	CONSTRAINT networks_pkey PRIMARY KEY (slug)
);