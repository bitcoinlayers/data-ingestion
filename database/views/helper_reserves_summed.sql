-- public.helper_reserves_summed source

CREATE OR REPLACE VIEW public.helper_reserves_summed
AS SELECT date,
    derivative_token,
    sum(balance) AS total_balance,
    collateral_token
   FROM reserve_balances
  GROUP BY date, reserve_network, collateral_token, derivative_token;