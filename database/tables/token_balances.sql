-- public.token_balances definition

-- Drop table

-- DROP TABLE public.token_balances;

CREATE TABLE public.token_balances (
	"date" date NOT NULL,
	token_implementation varchar(255) NULL,
	balance numeric(20, 8) NOT NULL,
	created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	id serial4 NOT NULL,
	CONSTRAINT token_balances_pkey PRIMARY KEY (id),
	CONSTRAINT token_balances_token_implementation_date_key UNIQUE (token_implementation, date),
	CONSTRAINT token_balances_token_implementation_fkey FOREIGN KEY (token_implementation) REFERENCES public.token_implementations(slug) ON DELETE CASCADE
);