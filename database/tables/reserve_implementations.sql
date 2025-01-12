-- public.reserve_implementations definition

-- Drop table

-- DROP TABLE public.reserve_implementations;

CREATE TABLE public.reserve_implementations (
	slug varchar(255) NOT NULL,
	reserve_address varchar(255) NULL,
	reserve_network varchar(255) NULL,
	derivative_token varchar(255) NULL,
	collateral_token varchar(255) NULL,
	tag text NULL,
	id serial4 NOT NULL,
	CONSTRAINT reserve_implementations_pkey PRIMARY KEY (id),
	CONSTRAINT reserve_implementations_reserve_address_collateral_token_key UNIQUE (reserve_address, collateral_token),
	CONSTRAINT reserve_implementations_collateral_token_fkey FOREIGN KEY (collateral_token) REFERENCES public.token_implementations(slug) ON DELETE CASCADE,
	CONSTRAINT reserve_implementations_derivative_token_fkey FOREIGN KEY (derivative_token) REFERENCES public.token_implementations(slug) ON DELETE CASCADE
);