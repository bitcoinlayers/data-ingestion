# API Endpoints

Here are the API endpoints for

- top gainers i.e., fastest growing tokens
- current supplies i.e., latest BTC supply value
- historical supplies i.e., BTC supplies over time
- reserves i.e., reserves balances for wrapped BTC assets

## Top Gainers

Percent Growth:

`https://api.btc-locked.com/top_gainers_by_tokenimpl`

Raw Supply Growth:

`https://api.btc-locked.com/top_gainers_by_tokenimpl_absolute`

Params to filter by time period:

```
?period=eq.daily
?period=eq.weekly
?period=eq.monthly
?period=eq.yearly
```

## Current Supply Rankings

By tokenimpl (e.g., BitGo-wBTC_Ethereum):

`https://api.btc-locked.com/current_supplies_by_tokenimpl`

By tokenproject (e.g., BitGo-wBTC):

`https://api.btc-locked.com/current_supplies_by_tokenproject`

By network (e.g., Ethereum):

`https://api.btc-locked.com/current_supplies_by_network`

Note that these supplies are adjusted for bridging. For example, the ~10k wBTC bridged from Ethereum to Arbitrum will count for

tokenimpl == BitGo-wBTC_Arbitrum

network == Arbitrum

it will not count for

tokenimpl == BitGo-wBTC_Ethereum

network == Ethereum

this avoids double counting on

tokenproject == BitGo-wBTC

## Historical Supply Data

By tokenimpl (e.g., BitGo-wBTC_Ethereum):

`https://api.btc-locked.com/historical_supplies_by_tokenimpl`

By tokenproject (e.g., BitGo-wBTC):

`https://api.btc-locked.com/historical_supplies_by_tokenproject`

By network (e.g., Ethereum):

`https://api.btc-locked.com/historical_supplies_by_network`

Staking supply only (e.g., Babylon):

`https://api.btc-locked.com/historical_supplies_by_staking`

Liquid staking tokens (LSTs) only (e.g., Lombard LBTC for Babylon staking, SolvBTC.CORE for Core staking):

`https://api.btc-locked.com/historical_supplies_by_liquidstaking`

Note that these supplies are all adjusted for bridging. For example, the ~10k wBTC bridged from Ethereum to Arbitrum will count for

tokenimpl == BitGo-wBTC_Arbitrum

network == Arbitrum

it will not count for

tokenimpl == BitGo-wBTC_Ethereum

network == Ethereum

this avoids double counting on

tokenproject == BitGo-wBTC

## Reserves

WORK IN PROGRESS

Note that our mapping and ingestion for reserves is not yet completed! There IS impartial data in the following endpoint. In the spirit of opensource, it's public regardless.

By tokenimpl with reserves:

`https://api.btc-locked.com/current_by_tokenimpl`
