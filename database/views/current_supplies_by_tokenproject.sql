-- public.current_supplies_by_tokenproject source

CREATE OR REPLACE VIEW public.current_supplies_by_tokenproject
AS SELECT token_slug,
    token_name,
    sum(balance) AS total_balance,
    max(date) AS latest_date,
    array_agg(DISTINCT network_slug) AS networks,
    array_agg(DISTINCT network_name) AS network_names,
    array_agg(DISTINCT token_address) AS token_addresses,
    rank() OVER (ORDER BY (sum(balance)) DESC) AS rank
   FROM current_supplies_by_tokenimpl
  GROUP BY token_slug, token_name
  ORDER BY (sum(balance)) DESC;