-- public.historical_supplies_by_network source

CREATE OR REPLACE VIEW public.historical_supplies_by_network
AS SELECT n.slug AS network_slug,
    n.name AS network_name,
    sum(tb.balance) AS total_balance,
    tb.date,
    array_agg(DISTINCT ti.token) AS tokens,
    array_agg(DISTINCT nt.name) AS token_names
   FROM helper_token_balances_adjusted tb
     JOIN token_implementations ti ON tb.token_implementation::text = ti.slug::text
     JOIN networks n ON ti.network::text = n.slug::text
     JOIN tokens nt ON ti.token::text = nt.slug::text
  WHERE nt.depegged = false AND nt.type <> 'staking'::token_type_enum
  GROUP BY tb.date, n.slug, n.name
  ORDER BY tb.date DESC, (sum(tb.balance)) DESC;