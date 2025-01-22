# API Endpoints

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

### Reserves

WORK IN PROGRESS

Note that our mapping and ingestion for reserves is not yet completed! There IS impartial data in the following endpoint. In the spirit of opensource, it's public regardless.

By tokenimpl with reserves:

`https://api.btc-locked.com/current_by_tokenimpl`
