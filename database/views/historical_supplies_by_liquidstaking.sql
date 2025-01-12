-- public.historical_supplies_by_liquidstaking source

CREATE OR REPLACE VIEW public.historical_supplies_by_liquidstaking
AS SELECT ti.token AS token_slug,
    nt.name AS token_name,
    sum(tb.balance) AS total_balance,
    tb.date,
    array_agg(DISTINCT n.slug) AS networks,
    array_agg(DISTINCT n.name) AS network_names,
    array_agg(DISTINCT ti.token_address) AS token_addresses,
    rank() OVER (PARTITION BY tb.date ORDER BY (sum(tb.balance)) DESC) AS rank
   FROM helper_token_balances_adjusted tb
     JOIN token_implementations ti ON tb.token_implementation::text = ti.slug::text
     JOIN networks n ON ti.network::text = n.slug::text
     JOIN tokens nt ON ti.token::text = nt.slug::text
  WHERE nt.depegged = false AND nt.type = 'liquid_staking'::token_type_enum
  GROUP BY tb.date, ti.token, nt.name
  ORDER BY tb.date DESC, (sum(tb.balance)) DESC;