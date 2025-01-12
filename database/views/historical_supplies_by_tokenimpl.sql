-- public.historical_supplies_by_tokenimpl source

CREATE OR REPLACE VIEW public.historical_supplies_by_tokenimpl
AS SELECT n.name AS network_name,
    n.slug AS network_slug,
    nt.name AS token_name,
    concat(n.slug, '_', ti.token) AS identifier,
    tb.balance AS amount,
    tb.date,
    nt.slug AS infra_slug,
    COALESCE(cs.rank, 0::bigint) AS network_rank,
    COALESCE(cp.rank, 0::bigint) AS project_rank
   FROM helper_token_balances_adjusted tb
     LEFT JOIN token_implementations ti ON tb.token_implementation::text = ti.slug::text
     LEFT JOIN networks n ON ti.network::text = n.slug::text
     LEFT JOIN tokens nt ON ti.token::text = nt.slug::text
     LEFT JOIN current_supplies_by_network cs ON n.slug::text = cs.network_slug::text
     LEFT JOIN current_supplies_by_tokenproject cp ON ti.token::text = cp.token_slug::text
  WHERE nt.depegged = false
  ORDER BY cp.rank DESC NULLS LAST, cs.rank DESC NULLS LAST, tb.date DESC;