-- public.current_supplies_by_network source

CREATE OR REPLACE VIEW public.current_supplies_by_network
AS SELECT network_slug,
    network_name,
    sum(balance) AS total_balance,
    max(date) AS latest_date,
    array_agg(DISTINCT token_slug) AS tokens,
    array_agg(DISTINCT token_name) AS token_names,
    array_agg(DISTINCT token_address) AS token_addresses,
    rank() OVER (ORDER BY (sum(balance)) DESC) AS rank
   FROM current_supplies_by_tokenimpl
  GROUP BY network_slug, network_name
  ORDER BY (sum(balance)) DESC;