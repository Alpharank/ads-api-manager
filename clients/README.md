# Client Dashboard Tokens & URLs

Each client has a token-gated dashboard deployed to GitHub Pages. The token is a SHA-256 hash derived from the client's Google Ads account ID.

No login is required: a valid token loads the dashboard; an invalid token shows an error.

## URL Pattern

```
https://alpharank.github.io/Google-Ads-Automation/?client={CLIENT_TOKEN}
```

## Client Tokens

| Client | Client ID | Token | Enriched? |
|--------|-----------|-------|:---------:|
| Altura Ad Account | `altura_ad_account` | `0dd9606ac3b4801f14798f39da0da59a752536fd82484564efbf51d97049a805` | No |
| California Coast Credit Union | `californiacoast_cu` | `0f72e02a2ecb57f724288d7dab4fbbdd2d1640b45ad59e34ff260ed591dad92f` | Yes |
| CommonWealth One Federal Credit Union | `commonwealth_one_fcu` | `27579ec0b32f0584b26197dc8e9fb46a190d4ef97a7931a7d71ddd3356b57a5e` | No |
| First Commonwealth Bank | `first_commonwealth_bank` | `d5d3084b4125d4c6bc8cd83d97df8f20df5da8458caff16a897335a81070dcd2` | Yes |
| First Community Credit Union | `firstcommunity_cu` | `05979da34c7285484c85deed78e6fd75cefef705b3d9ec87f1c9cd9954475530` | Yes |
| Kitsap Credit Union | `kitsap_cu` | `d49d866d6e58accdfc6b0a57099c04d349ceafedcefa8f249d3cee7bd8ebb472` | Yes |
| Public Service Credit Union | `publicservice_cu` | `92ec66122ab956def432f3ba0d4f5f142db0f211270256bf12fc27beba80fd9a` | Yes |

## Dashboard URLs

```
# Altura Ad Account
https://alpharank.github.io/Google-Ads-Automation/?client=0dd9606ac3b4801f14798f39da0da59a752536fd82484564efbf51d97049a805

# California Coast Credit Union
https://alpharank.github.io/Google-Ads-Automation/?client=0f72e02a2ecb57f724288d7dab4fbbdd2d1640b45ad59e34ff260ed591dad92f

# CommonWealth One Federal Credit Union
https://alpharank.github.io/Google-Ads-Automation/?client=27579ec0b32f0584b26197dc8e9fb46a190d4ef97a7931a7d71ddd3356b57a5e

# First Commonwealth Bank
https://alpharank.github.io/Google-Ads-Automation/?client=d5d3084b4125d4c6bc8cd83d97df8f20df5da8458caff16a897335a81070dcd2

# First Community Credit Union
https://alpharank.github.io/Google-Ads-Automation/?client=05979da34c7285484c85deed78e6fd75cefef705b3d9ec87f1c9cd9954475530

# Kitsap Credit Union
https://alpharank.github.io/Google-Ads-Automation/?client=d49d866d6e58accdfc6b0a57099c04d349ceafedcefa8f249d3cee7bd8ebb472

# Public Service Credit Union
https://alpharank.github.io/Google-Ads-Automation/?client=92ec66122ab956def432f3ba0d4f5f142db0f211270256bf12fc27beba80fd9a
```

## Assembly Integration

Embed in Assembly iFrame tabs:

```html
<iframe src="https://alpharank.github.io/Google-Ads-Automation/?client={CLIENT_TOKEN}" />
```

## Adding New Clients

New clients are auto-onboarded when added to the MCC. The pipeline generates a slug and SHA-256 token automatically. To enable funded metrics for a new client, see the [main README](../README.md#adding-a-new-client).
