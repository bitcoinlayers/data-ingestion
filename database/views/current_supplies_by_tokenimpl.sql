-- public.current_supplies_by_tokenimpl source

CREATE OR REPLACE VIEW public.current_supplies_by_tokenimpl
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
        ), adjustments AS (
         SELECT hdb.token_implementation AS original_token_implementation,
            sum(rb.balance) AS total_to_subtract
           FROM helper_dependencies_bridges hdb
             JOIN recent_balances rb ON rb.token_implementation::text = ANY (hdb.tokens_to_subtract::text[])
          GROUP BY hdb.token_implementation
        ), adjusted_balances AS (
         SELECT rb.token_implementation,
                CASE
                    WHEN adj.total_to_subtract IS NOT NULL THEN
                    CASE
                        WHEN rb.token_implementation::text = adj.original_token_implementation THEN rb.balance - adj.total_to_subtract
                        ELSE rb.balance
                    END
                    ELSE rb.balance
                END AS balance,
            rb.date
           FROM recent_balances rb
             LEFT JOIN adjustments adj ON rb.token_implementation::text = adj.original_token_implementation
        )
 SELECT ab.token_implementation,
    ab.balance,
    ab.date,
    ti.network,
    n.slug AS network_slug,
    ti.network_origin,
    n.name AS network_name,
    ti.token AS token_slug,
    nt.name AS token_name,
    ti.token_address,
    n.explorer,
    rank() OVER (ORDER BY ab.balance DESC) AS rank
   FROM adjusted_balances ab
     JOIN token_implementations ti ON ab.token_implementation::text = ti.slug::text
     JOIN networks n ON ti.network::text = n.slug::text
     JOIN tokens nt ON ti.token::text = nt.slug::text
  ORDER BY ab.balance DESC;