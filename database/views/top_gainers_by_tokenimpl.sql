-- public.top_gainers_by_tokenimpl source

CREATE OR REPLACE VIEW public.top_gainers_by_tokenimpl
AS WITH recent_balances AS (
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
         SELECT tb.token_implementation,
            tb.balance,
            tb.date,
                CASE
                    WHEN tb.date = (CURRENT_DATE - '1 day'::interval) THEN 'daily'::text
                    WHEN tb.date = (CURRENT_DATE - '7 days'::interval) THEN 'weekly'::text
                    WHEN tb.date = (CURRENT_DATE - '1 mon'::interval) THEN 'monthly'::text
                    WHEN tb.date = (CURRENT_DATE - '1 year'::interval) THEN 'yearly'::text
                    ELSE NULL::text
                END AS period
           FROM token_balances tb
          WHERE tb.date = ANY (ARRAY[CURRENT_DATE - '1 day'::interval, CURRENT_DATE - '7 days'::interval, CURRENT_DATE - '1 mon'::interval, CURRENT_DATE - '1 year'::interval])
        ), percent_changes AS (
         SELECT r.token_implementation,
            r.balance AS recent_balance,
            p.balance AS past_balance,
            COALESCE(round((r.balance - p.balance) / NULLIF(p.balance, 0::numeric) * 100::numeric, 2), 0::numeric) AS percent_change,
            r.date,
            p.period
           FROM recent_balances r
             LEFT JOIN past_balances p ON r.token_implementation::text = p.token_implementation::text
        )
 SELECT pc.token_implementation,
    pc.recent_balance,
    pc.past_balance,
    pc.percent_change,
    pc.period,
    pc.date,
    ti.network,
    n.slug AS network_slug,
    ti.network_origin,
    n.name AS network_name,
    ti.token AS token_slug,
    nt.name AS token_name,
    ti.token_address,
    n.explorer,
    rank() OVER (PARTITION BY pc.period ORDER BY pc.percent_change DESC) AS rank
   FROM percent_changes pc
     JOIN token_implementations ti ON pc.token_implementation::text = ti.slug::text
     JOIN networks n ON ti.network::text = n.slug::text
     JOIN tokens nt ON ti.token::text = nt.slug::text
  WHERE pc.percent_change IS NOT NULL
  ORDER BY pc.period, pc.percent_change DESC;