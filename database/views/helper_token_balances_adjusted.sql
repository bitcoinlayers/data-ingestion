-- public.helper_token_balances_adjusted source

CREATE OR REPLACE VIEW public.helper_token_balances_adjusted
AS WITH adjustments AS (
         SELECT hdb.token_implementation AS original_token_implementation,
            tb.date,
            sum(tb.balance) AS total_to_subtract
           FROM helper_dependencies_bridges hdb
             JOIN token_balances tb ON tb.token_implementation::text = ANY (hdb.tokens_to_subtract::text[])
          GROUP BY hdb.token_implementation, tb.date
        ), adjusted_balances AS (
         SELECT tb.token_implementation,
            tb.date,
                CASE
                    WHEN adj.total_to_subtract IS NOT NULL THEN
                    CASE
                        WHEN tb.token_implementation::text = adj.original_token_implementation THEN tb.balance - adj.total_to_subtract
                        ELSE tb.balance
                    END
                    ELSE tb.balance
                END::numeric(20,8) AS balance,
            tb.created_at,
            tb.id
           FROM token_balances tb
             LEFT JOIN adjustments adj ON tb.token_implementation::text = adj.original_token_implementation AND tb.date = adj.date
        )
 SELECT date,
    token_implementation,
    balance,
    created_at,
    id
   FROM adjusted_balances ab
  ORDER BY date DESC, token_implementation;