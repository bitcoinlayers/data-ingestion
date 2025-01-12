-- public.reserve_balances definition

-- Drop table

-- DROP TABLE public.reserve_balances;

CREATE TABLE public.reserve_balances (
	"date" date NOT NULL,
	balance numeric(20, 8) NOT NULL,
	created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	reserve_implementation_id int4 NULL,
	reserve_network varchar(255) NULL,
	collateral_token varchar(255) NULL,
	derivative_token varchar(255) NULL,
	reserve_address varchar(255) NOT NULL,
	CONSTRAINT unique_reserve_balance_entry UNIQUE (date, reserve_network, reserve_address, collateral_token, derivative_token),
	CONSTRAINT reserve_balances_reserve_implementation_fkey FOREIGN KEY (reserve_implementation_id) REFERENCES public.reserve_implementations(id) ON DELETE CASCADE
);