-- public.current_prices source

CREATE OR REPLACE VIEW public.current_prices
AS WITH latest_prices AS (
         SELECT price.token_slug,
            price.token_name,
            price.price_usd,
            price.date,
            row_number() OVER (PARTITION BY price.token_slug ORDER BY price.date DESC) AS rn
           FROM price
        )
 SELECT token_slug,
    token_name,
    price_usd,
    date AS most_recent_date
   FROM latest_prices
  WHERE rn = 1
  ORDER BY token_slug;