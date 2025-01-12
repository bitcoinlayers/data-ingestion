-- public.current_reserves_by_tokenimpl source

CREATE OR REPLACE VIEW public.current_reserves_by_tokenimpl
AS WITH recent_reserves AS (
         SELECT hrs.derivative_token AS token_implementation,
            hrs.total_balance AS balance,
            hrs.date
           FROM helper_reserves_summed hrs
          WHERE hrs.date = (( SELECT max(inner_hrs.date) AS max
                   FROM helper_reserves_summed inner_hrs
                  WHERE inner_hrs.derivative_token::text = hrs.derivative_token::text))
        ), adjustments AS (
         SELECT hdb.token_implementation AS original_token_implementation,
            sum(rr.balance) AS total_to_subtract
           FROM helper_dependencies_bridges hdb
             JOIN recent_reserves rr ON rr.token_implementation::text = ANY (hdb.tokens_to_subtract::text[])
          GROUP BY hdb.token_implementation
        ), adjusted_reserves AS (
         SELECT rr.token_implementation,
                CASE
                    WHEN adj.total_to_subtract IS NOT NULL THEN
                    CASE
                        WHEN rr.token_implementation::text = adj.original_token_implementation THEN rr.balance - adj.total_to_subtract
                        ELSE rr.balance
                    END
                    ELSE rr.balance
                END AS balance,
            rr.date
           FROM recent_reserves rr
             LEFT JOIN adjustments adj ON rr.token_implementation::text = adj.original_token_implementation
        )
 SELECT ar.token_implementation,
    ar.balance,
    ar.date,
    ti.network,
    n.slug AS network_slug,
    ti.network_origin,
    n.name AS network_name,
    ti.token AS token_slug,
    nt.name AS token_name,
    ti.token_address,
    n.explorer,
    rank() OVER (ORDER BY ar.balance DESC) AS rank
   FROM adjusted_reserves ar
     JOIN token_implementations ti ON ar.token_implementation::text = ti.slug::text
     JOIN networks n ON ti.network::text = n.slug::text
     JOIN tokens nt ON ti.token::text = nt.slug::text
  ORDER BY ar.balance DESC;