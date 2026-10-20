# BoardReady

Month-end close and investor update for a startup on Seapoint, built so that **no number is
ever typed by the model**.

For month M it pulls M and the 3 prior months from Seapoint, lists what isn't ready for the
accountant, explains material spend changes down to the transaction, proposes fixes a person
approves, and drafts the investor update with every figure filled in by code.

## Run it

```
cd boardready
python server.py            # http://localhost:8765
```
Needs Python 3 (standard library only) and the `claude` CLI logged in with the Seapoint Staging
connector. Optional: `BOARDREADY_ORG=<org id>` (default: Hackathon - R3CON Tech, GBP).

- CLI: `python pipeline.py 2026-09 --a-only` prints metrics; `python pipeline.py 2026-09` runs A+B;
  `python pipeline.py <run_id> --resume` re-runs B on a saved pull.
- Tests (offline): `python -m unittest test_pipeline`
- A full run takes about 5 minutes. **Replay → "★ demo"** loads a saved run instantly.

## How it works

| Step | Who | Tools allowed |
|---|---|---|
| A: data pull | `claude -p` agent, given the EXACT args for 19 calls built by `pipeline.py` | get_current_user, query_*, MCP resources |
| metrics | Python, from the raw tool results in `runs/<id>/stream_a.jsonl` | – |
| B: investigate + draft | `claude -p` agent, writes `[[placeholders]]` only | same read-only list |
| fill + verify | Python | – |
| C: apply a fix | `claude -p`, only after a click on Approve, given exact ids + codes | set_transaction_coding, approve_reconciliation **only** |

All runs use `--permission-mode dontAsk`, so any tool outside the allowlist is refused.

**Trust guarantees**
- Totals use Seapoint aggregate mode on `reporting_amount`, never paged rows. The only
  hand-summed figure is cash over account balances, which Seapoint's runway methodology requires.
- Burn and runway follow `METHODOLOGY.md`, copied from Seapoint's methodology resources, so the
  figures match the reporting page.
- Every metric records the `tool_use_id`, tool and input it came from. Hover a figure in the UI
  to see it.
- After filling, any £/€/$ amount or % the model typed itself is flagged as **unverified**
  (shown in red), and unknown placeholders are flagged too.
- Evidence transaction ids that don't appear in a raw tool result are dropped. A proposed coding
  must be Seapoint's suggestion or a code pair already seen on other transactions.
- Sense checks: category spend adds up to total spend (±£1); every query's date range matches
  the window; the agent used exactly the planned arguments.
- Writes: server-side checks allow only exception ids from that run and plain-text codes. Every
  write is appended to `audit.jsonl` with time, tool, ids, before/after, our change_id and
  Seapoint's change_id.
- Transaction text is treated as data, never instructions (in every prompt).

## Files
`server.py` (API + static page) · `pipeline.py` (runs, parsing, metrics, fill, approve) ·
`index.html` · `prompts/` · `runs/<run_id>/` (stream_a/b/c jsonl, plan_a, metrics, run, status) ·
`audit.jsonl` · `METHODOLOGY.md` · `test_pipeline.py`

## Known limits
- The default month is the last full calendar month. Pick another in the month selector if
  that month is empty.
- "Needs review" uses Seapoint's documented filter, which is about the bookkeeping page's
  count, not exact (lock date and own-account transfers aren't excluded).
- Cash is today's balance, while burn is for month M.
- Exceptions without suggested codes get a drafted question, not an Approve button.

---

## 3-minute demo script

1. **Hook (15s).** "Closing the month and writing the investor update takes a founder a day,
   and you can't trust an LLM with numbers. BoardReady does it in 5 minutes, and the model
   never types a number."
2. **Replay ★ demo (20s).** Point at the top bar: readiness 97.5%, **53/53 figures verified**,
   three green sense checks. "Every figure traces back to a raw Seapoint response."
3. **Live run (start it now, 10s).** Click **Close month**. While it runs, the live list shows each
   Seapoint query. "Code wrote these exact arguments; the agent just executes them, read-only."
   Switch back to the demo run while it works.
4. **Not ready for the accountant (40s).** Six payments. Show the drafted Slack question and its
   Copy button. Click **Approve** on Meridian Insurance (433 / Exempt). "Only now does a separate
   agent run, allowed nothing but the two write tools and given the exact ids." The row turns
   ✓ synced, readiness goes up, and the write appears in the **Audit log** with before/after.
5. **What changed (40s).** Spend is up 22.5%. Open Payroll: three people paid twice. Then Rent:
   two extra WeWork direct debits. Expand the evidence transactions. "Possible duplicate
   payments, found by the close, each linked to its transactions."
6. **Investor update (30s).** Hover £2,459,110 to show the exact query. "The model wrote
   `[[cash]]`; code filled it. If it had typed a number itself, it would show up red here."
   Click Copy.
7. **Close (15s).** Back to the live run, which should be done or nearly done by now. "Read-only
   by default, writes only on a click, every write audited."

If the live run fails or is slow, everything above works from ★ demo.
