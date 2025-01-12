-- public.tokens definition

-- Drop table

-- DROP TABLE public.tokens;

CREATE TABLE public.tokens (
	slug varchar(255) NOT NULL,
	"name" text NOT NULL,
	project_name varchar(255) NULL,
	project_slug varchar(255) NULL,
	token_name varchar(255) NULL,
	token_slug varchar(255) NULL,
	"type" public."token_type_enum" NULL,
	created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	depegged bool DEFAULT false NULL,
	CONSTRAINT tokens_pkey PRIMARY KEY (slug),
	CONSTRAINT tokens_project_slug_token_slug_key UNIQUE (project_slug, token_slug)
);