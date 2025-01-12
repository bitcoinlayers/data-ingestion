-- public.helper_current_supplies_by_tokenimpl_raw source
-- Raw values for tokenimpl, without adjusting for double count from bridging or other

CREATE OR REPLACE VIEW public.helper_current_supplies_by_tokenimpl_raw
AS WITH recent_balances AS (
         SELECT tb.token_implementation,
            tb.balance,
            tb.date
           FROM token_balances tb
          WHERE tb.date = (( SELECT max(inner_tb.date) AS max
                   FROM token_balances inner_tb
                  WHERE inner_tb.token_implementation::text = tb.token_implementation::text))
        )
 SELECT rb.token_implementation,
    rb.balance,
    rb.date,
    ti.network,
    n.slug AS network_slug,
    ti.network_origin,
    n.name AS network_name,
    ti.token AS token_slug,
    nt.name AS token_name,
    ti.token_address,
    n.explorer,
    rank() OVER (ORDER BY rb.balance DESC) AS rank
   FROM recent_balances rb
     JOIN token_implementations ti ON rb.token_implementation::text = ti.slug::text
     JOIN networks n ON ti.network::text = n.slug::text
     JOIN tokens nt ON ti.token::text = nt.slug::text
  ORDER BY rb.balance DESC;