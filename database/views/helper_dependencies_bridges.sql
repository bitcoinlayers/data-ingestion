-- public.helper_dependencies_bridges source
-- Show bridge dependencies for each token implementation in order to adjust for double counting

CREATE OR REPLACE VIEW public.helper_dependencies_bridges
AS WITH origin_tokens AS (
         SELECT ti.token AS token_slug,
            ti.network_origin AS origin_network,
            array_agg(ti.slug) AS tokens_to_subtract
           FROM token_implementations ti
          WHERE ti.network_origin IS NOT NULL AND ti.network_origin::text <> 'native'::text AND ti.token_address::text ~ '^[0-9].*'::text
          GROUP BY ti.token, ti.network_origin
        )
 SELECT nt.slug AS token_slug,
    concat(nt.slug, '_', origin_tokens.origin_network) AS token_implementation,
    nt.token_name,
    nt.type AS token_type,
    origin_tokens.origin_network,
    origin_tokens.tokens_to_subtract
   FROM origin_tokens
     JOIN tokens nt ON origin_tokens.token_slug::text = nt.slug::text
  WHERE nt.depegged = false;