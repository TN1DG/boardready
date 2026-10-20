# BoardReady methodology

Copied from Seapoint's MCP resources `seapoint://finance/methodology/burn-rate` and
`seapoint://finance/methodology/runway` (read 2026-10-20). `pipeline.py` implements
these formulas exactly.

The user's saved preference is `burnMode=gross, burnBasis=last_month` (the default, since no
backend preference is stored). For month M, the "last month" basis is month M itself.

## Monthly burn (one calendar month)

```
settled        = rows whose status ∉ {FAILED, CANCELLED, FULL_REVERSAL}   (PENDING counts)
debits         = Σ reporting_amount over settled debit rows
refundCredits  = Σ reporting_amount over settled credit rows with type ∈ {REFUND, CARD_REFUND, CARD_CHARGEBACK}
revenueCredits = Σ reporting_amount over settled credit rows that are NOT refunds and NOT
                 category "Financial Items › Capital Investments"   (uncategorised credits count)

gross burn = -(debits - refundCredits)
net burn   = -(debits - refundCredits - revenueCredits)
```

Always excluded: internal transfers (the tool default, `include_internal_transfers` not set) and
transactions flagged excluded from runway (`include_excluded_from_runway: false` on every burn query).
A net burn > 0 is a net inflow: report "no burn" (the headline is clamped to 0).

Queries (aggregate mode, `reporting_amount`, never paged rows):
- `burn_out` / gross: where `{date range, not:{status:[FAILED,CANCELLED,FULL_REVERSAL]},
  or:[{direction:debit},{direction:credit,type:[REFUND,CARD_REFUND,CARD_CHARGEBACK]}]}`,
  grouped by month and direction. debit group = debits, credit group = refundCredits.
- `burn_in` / revenue: where `{date range, direction:credit, not:{status:[...]},
  and:[{not:{type:[REFUND,CARD_REFUND,CARD_CHARGEBACK]}}]}` grouped by month and category_id.
  Code drops the Capital Investments category ids (looked up from `query_categories`); the
  rest is revenueCredits. This matches putting the capital-investment `not` on the revenue branch only.

## Cash (runway numerator)

Sum over `query_accounts` rows: `reporting_balance.cash_position` for BANKING, `reporting_balance.total`
for INVESTMENT. Accounts missing `reporting_balance` are excluded and counted. This is the one
figure we add up ourselves, as the methodology says. Card accounts reporting 0 may hide debt.

## Runway

```
runway_months = cash / |net burn of the basis month|
```
Runway always uses **net** burn, whatever the burn mode. If net burn >= -0.01, runway is
unlimited ("No limit") and there is no cash-zero date. Cash-zero date = end of the current
period plus the fractional runway months (not rounded first).

## Completeness

If any aggregate group has `money.<metric>.complete == false`, the total is under-counted; we
record a warning in the run notes.

## Other BoardReady figures

- Spend (category and vendor tables): BOOKED debits, `include_excluded_from_runway: false`, sum of reporting_amount.
- Material change: |M − prior-3 average| > £500 AND > 20%; top 5 by |change £|.
- Readiness % = 1 − needs_review_n / payments_n (booked debits with amount > 0 in M).
