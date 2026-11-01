# AI usage

**Tool:** Claude Code (Claude Opus 5.5) in the terminal.

**How it was used**
1. Read the assignment brief and produced an implementation plan with milestones (`PLAN.md`).
2. Generated the scaffold, the extract, silver/gold SQL, DQ and test code from that plan.
3. Ran the tests and the live pipeline, then fixed issues the runs surfaced (for example, a DuckDB "unfinished result" error in the query step, fixed by fully fetching results).

**What I reviewed and own**
- Architecture and layer contracts (bronze immutability, dedupe key, idempotency).
- Verified the outputs: 1,680 silver hourly rows (5 cities × 14 days × 24 h), all 8 DQ checks passing, 4 tests passing.

Screenshots of the conversations are in `docs/ai-evidence/`.
