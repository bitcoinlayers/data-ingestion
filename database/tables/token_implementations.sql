-- public.token_implementations definition

-- Drop table

-- DROP TABLE public.token_implementations;

CREATE TABLE public.token_implementations (
	slug varchar(255) NOT NULL,
	"name" varchar(255) NULL,
	"token" varchar(255) NULL,
	network varchar(255) NULL,
	network_origin varchar(255) NULL,
	token_decimals varchar(255) NULL,
	token_address varchar(255) NULL,
	reserve_implementations jsonb NULL,
	"degree" int4 NULL,
	CONSTRAINT token_implementations_pkey PRIMARY KEY (slug),
	CONSTRAINT token_implementations_network_fkey FOREIGN KEY (network) REFERENCES public.networks(slug) ON DELETE CASCADE,
	CONSTRAINT token_implementations_network_origin_fkey FOREIGN KEY (network_origin) REFERENCES public.networks(slug) ON DELETE CASCADE,
	CONSTRAINT token_implementations_token_fkey FOREIGN KEY ("token") REFERENCES public.tokens(slug) ON DELETE CASCADE
);

-- Table Triggers

create trigger generate_slug_and_name before
insert
    or
update
    on
    public.token_implementations for each row execute function auto_generate_slug_and_name();