-- public.top_gainers_by_tokenimpl_absolute source

CREATE OR REPLACE VIEW public.top_gainers_by_tokenimpl_absolute
AS WITH periods AS (
         SELECT 'daily'::text AS period,
            CURRENT_DATE - '1 day'::interval AS date
        UNION ALL
         SELECT 'weekly'::text AS period,
            CURRENT_DATE - '7 days'::interval AS date
        UNION ALL
         SELECT 'monthly'::text AS period,
            CURRENT_DATE - '1 mon'::interval AS date
        UNION ALL
         SELECT 'yearly'::text AS period,
            CURRENT_DATE - '1 year'::interval AS date
        ), token_periods AS (
         SELECT ti_1.slug AS token_implementation,
            p.period,
            p.date
           FROM token_implementations ti_1
             CROSS JOIN periods p
        ), recent_balances AS (
         SELECT tb.token_implementation,
            tb.balance,
            tb.date
           FROM token_balances tb
             JOIN token_implementations ti_1 ON tb.token_implementation::text = ti_1.slug::text
             JOIN tokens nt_1 ON ti_1.token::text = nt_1.slug::text
          WHERE tb.date = (( SELECT max(inner_tb.date) AS max
                   FROM token_balances inner_tb
                  WHERE inner_tb.token_implementation::text = tb.token_implementation::text)) AND nt_1.depegged = false
        ), past_balances AS (
         SELECT tp.token_implementation,
            tp.period,
            tp.date,
            COALESCE(tb.balance, 0::numeric) AS balance
           FROM token_periods tp
             LEFT JOIN token_balances tb ON tp.token_implementation::text = tb.token_implementation::text AND tb.date = tp.date
        ), supply_changes AS (
         SELECT r.token_implementation,
            r.balance AS recent_balance,
            COALESCE(p.balance, 0::numeric) AS past_balance,
            r.balance - COALESCE(p.balance, 0::numeric) AS supply_change,
            p.period,
            r.date
           FROM recent_balances r
             LEFT JOIN past_balances p ON r.token_implementation::text = p.token_implementation::text
        )
 SELECT sc.token_implementation,
    sc.recent_balance,
    sc.past_balance,
    sc.supply_change,
    sc.period,
    sc.date,
    ti.network,
    n.slug AS network_slug,
    ti.network_origin,
    n.name AS network_name,
    ti.token AS token_slug,
    nt.name AS token_name,
    ti.token_address,
    n.explorer,
    rank() OVER (PARTITION BY sc.period ORDER BY sc.supply_change DESC) AS rank
   FROM supply_changes sc
     JOIN token_implementations ti ON sc.token_implementation::text = ti.slug::text
     JOIN networks n ON ti.network::text = n.slug::text
     JOIN tokens nt ON ti.token::text = nt.slug::text
  ORDER BY sc.period, sc.supply_change DESC;