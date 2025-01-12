-- public.current_by_tokenimpl source
-- Create a view with the current supply and reserve balances for each token implementation

CREATE OR REPLACE VIEW public.current_by_tokenimpl
AS WITH supply_data AS (
         SELECT cs.token_implementation,
            cs.balance AS supply_balance,
            cs.date AS supply_date,
            cs.network,
            cs.network_slug,
            cs.network_origin,
            cs.network_name,
            cs.token_slug,
            cs.token_name,
            cs.token_address,
            cs.explorer
           FROM helper_current_supplies_by_tokenimpl_raw cs
        ), reserve_data AS (
         SELECT cr.token_implementation,
            cr.balance AS reserve_balance,
            cr.date AS reserve_date
           FROM current_reserves_by_tokenimpl cr
        )
 SELECT sd.token_implementation,
    sd.supply_balance,
    rd.reserve_balance,
    sd.supply_balance - COALESCE(rd.reserve_balance, 0::numeric) AS balance_difference,
    GREATEST(sd.supply_date, rd.reserve_date) AS latest_date,
    sd.network,
    sd.network_slug,
    sd.network_origin,
    sd.network_name,
    sd.token_slug,
    sd.token_name,
    sd.token_address,
    sd.explorer,
    rank() OVER (ORDER BY sd.supply_balance DESC) AS rank
   FROM supply_data sd
     LEFT JOIN reserve_data rd ON sd.token_implementation::text = rd.token_implementation::text
  ORDER BY sd.supply_balance DESC;