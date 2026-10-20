"""Offline checks of the safety logic (no Seapoint calls).  python -m unittest test_pipeline"""
import unittest

import pipeline

SRC = {"tool_use_id": "t1", "tool": "query_accounts", "input": {}}
METRICS = {"cash": {"name": "cash", "value": 2459110.4, "unit": "GBP", "label": "cash", "source": SRC},
           "spend_total_change_pct": {"name": "spend_total_change_pct", "value": 22.5, "unit": "pct",
                                      "label": "x", "source": SRC}}


class Fill(unittest.TestCase):
    def test_placeholders_filled_from_metrics(self):
        text, segs, unv, used = pipeline.fill("Cash is [[cash]], spend [[spend_total_change_pct]].", METRICS)
        self.assertEqual(text, "Cash is £2,459,110, spend +22.5%.")
        self.assertEqual(unv, [])
        self.assertEqual(used, ["cash", "spend_total_change_pct"])

    def test_model_typed_numbers_flagged(self):
        _, _, unv, _ = pipeline.fill("We spent £5,000 more, up 12% and $3k on ads.", METRICS)
        self.assertEqual(unv, ["£5,000", "12%", "$3k"])

    def test_unknown_placeholder_flagged(self):
        _, segs, unv, _ = pipeline.fill("Burn was [[made_up]].", METRICS)
        self.assertEqual(unv, ["[[made_up]]"])
        self.assertTrue(any(s.get("unknown") for s in segs))


class Window(unittest.TestCase):
    def test_window(self):
        w = pipeline.window("2026-01")
        self.assertEqual(w["start"], "2025-10-01")
        self.assertEqual(w["end"], "2026-02-01")
        self.assertEqual(w["months"], ["2025-10-01", "2025-11-01", "2025-12-01", "2026-01-01"])

    def test_every_dated_query_in_window(self):
        w = pipeline.window("2026-09")
        for key, (tool, args) in pipeline.plan_a("2026-09").items():
            d = (args.get("where") or {}).get("date")
            if d:
                self.assertGreaterEqual(d["gte"], w["start"], key)
                self.assertLessEqual(d["lt"], w["end"], key)

    def test_burn_queries_exclude_runway_flagged(self):
        plan = pipeline.plan_a("2026-09")
        for key in ("A5_burn_out", "A5_burn_in"):
            self.assertIs(plan[key][1]["include_excluded_from_runway"], False)
            self.assertEqual(plan[key][1]["where"]["not"], {"status": pipeline.NON_SETTLING})


class ApproveGuard(unittest.TestCase):
    def test_rejects_ids_not_in_run(self):
        with self.assertRaises(ValueError):
            pipeline.approve("demo", ["txn_not_an_exception"], "461", "INPUT2")

    def test_rejects_odd_codes(self):
        with self.assertRaises(ValueError):
            pipeline.approve("demo", ["txn_31f1k1xm85k3mqs6p2yjn0mpne"], "433; rm -rf", "X")


if __name__ == "__main__":
    unittest.main()
