# RiskCore PD Engine — Technical Reference

> **Audience.** Two readers in one document.
> **Engineers** who need to read, extend, deploy, and operate the codebase will find architecture, code paths, schemas, and runbooks.
> **Risk managers, model validators, and supervisors** (EBA, BaFin, PRA, OCC) who need to understand how decisions are made, what is logged, and where evidence lives will find the regulatory framing, audit-trail layout, and data contracts.
>
> Where the two audiences diverge, sections are tagged **[Eng]** or **[Audit]**.
>
> **Scope.** Covers the full RiskCore PD Engine as it exists on `main`: the 3-agent pipeline, REST API, Postgres schema, R-script registry and LLM codegen pipeline, the React dashboard, deployment & operations, and all on-disk and on-the-wire data contracts.
>
> **Status.** This document is the authoritative engineering and audit reference for the application. The shorter `replit.md` at the repository root is a high-level orientation file intended for tooling — this document supersedes it where they overlap.

---

## Table of Contents

1.  [Executive Summary](#1-executive-summary)
2.  [System Architecture](#2-system-architecture)
3.  [The Multi-Agent PD Pipeline](#3-the-multi-agent-pd-pipeline)
4.  [REST API Reference](#4-rest-api-reference)
5.  [Database Schema](#5-database-schema)
6.  [R Script Registry & LLM Codegen](#6-r-script-registry--llm-codegen)
7.  [Frontend — `pd-dashboard`](#7-frontend--pd-dashboard)
8.  [Deployment & Operations Runbook](#8-deployment--operations-runbook)
9.  [Data Contracts](#9-data-contracts)
10. [Regulatory Mapping](#10-regulatory-mapping)
11. [Glossary](#11-glossary)

---

## 1. Executive Summary

The **RiskCore PD Engine** is a multi-agent system that takes a raw retail-credit CSV and produces a fully-audited Probability-of-Default (PD) model with three independent reports (EDA, Model Development, Independent Validation) and a regulator-ready audit trail.

It is deliberately structured around the **three lines of defence** required by EBA GL/2017/16, Basel III Principle 7, and BaFin MaRisk AT 4.1:

| Line of defence | Agent | Output |
| --- | --- | --- |
| 1st line — data ownership | **Data Analyzer** | Cleaned dataset + EDA report + handoff manifest |
| 1st line — model build | **Model Developer** | Trained GLM (logistic regression) + challenger model + dev report |
| 2nd line — independent validation | **Model Validator** | PSI / stability tests + IRB-style validation verdict |

Decisions made by the LLM (Anthropic `claude-sonnet-4-6`) are **decision-makers, not trainers**: every numerical computation (data quality, IV/WOE, GLM fitting, PSI) runs in **R 4.5** under a controlled runtime (a long-lived R worker for the trusted pipeline scripts; an isolated sandboxed `Rscript` invocation for LLM-generated scripts). The LLM only judges the R outputs, picks features, names the verdict, and writes prose. Every LLM rationale is persisted in the audit log.

The engine ships as three artifacts in a pnpm monorepo:

* `artifacts/api-server` — Express 5 backend + agent orchestrator (Node 24).
* `artifacts/pd-dashboard` — React + Vite operator UI.
* `artifacts/mockup-sandbox` — internal design sandbox (not a production surface).

---

## 2. System Architecture

### 2.1 Repository layout [Eng]

```text
artifacts-monorepo/
├── artifacts/
│   ├── api-server/                 # Express + agent orchestrator (kind=api)
│   │   └── src/
│   │       ├── agents/             # data-analyzer/, model-developer.ts,
│   │       │                       # model-validator.ts, orchestrator.ts,
│   │       │                       # feedback.ts, audit-logger.ts, run-events.ts
│   │       ├── routes/pipeline/    # REST handlers
│   │       ├── tools/              # r-runner.ts (persistent worker),
│   │       │                       # r-sandbox.ts (isolated codegen runner)
│   │       ├── r-scripts/          # data_quality.R, calculate_iv.R,
│   │       │                       # calculate_psi.R, train_model.R, impute.R
│   │       └── lib/logger.ts       # pino + pino-http
│   ├── pd-dashboard/               # React + Vite SPA (kind=web)
│   └── mockup-sandbox/             # design sandbox (kind=design)
├── lib/
│   ├── api-spec/                   # OpenAPI 3.1 + Orval codegen
│   ├── api-client-react/           # Generated React Query hooks
│   ├── api-zod/                    # Generated Zod schemas
│   ├── db/                         # Drizzle ORM + schema/ + drizzle/ migrations
│   └── integrations-anthropic-ai/  # Anthropic SDK wrapper (Replit AI proxy)
└── pnpm-workspace.yaml             # catalog pins, workspace discovery
```

The monorepo follows the **artifacts** convention: each artifact has a `.replit-artifact/artifact.toml` that registers the artifact with the platform's reverse proxy, declares its dev/prod commands, and pins its `localPort`.

### 2.2 Runtime topology [Eng]

```
                ┌──────────────────────────────────────────┐
                │             Replit reverse proxy         │
                │            ($REPLIT_DOMAINS over TLS)    │
                └──────────────────────────────────────────┘
                         │ /                     │ /api
                         ▼                       ▼
          ┌──────────────────────┐    ┌────────────────────────┐
          │ pd-dashboard (Vite)  │    │  api-server (Express)  │
          │  static SPA :21706   │    │  Node 24 :8080         │
          └──────────────────────┘    └────────────────────────┘
                                              │
                ┌─────────────────────────────┼──────────────────────────┐
                ▼                             ▼                          ▼
      ┌──────────────────┐          ┌──────────────────┐       ┌────────────────┐
      │  PostgreSQL      │          │  Filesystem      │       │  Anthropic API │
      │  (Replit DB)     │          │  data/uploads/   │       │  via AI proxy  │
      │  pipeline_runs   │          │  data/<runId>/   │       │  claude-       │
      │  agent_results   │          │   data-analyzer/ │       │  sonnet-4-6    │
      │  audit_log       │          │   model_*.rds    │       │  + caching     │
      │  generated_scripts│         │   *_report.md    │       └────────────────┘
      └──────────────────┘          └──────────────────┘
                                              │
                                              ▼
                                  ┌──────────────────────┐
                                  │  R 4.5               │
                                  │  ├─ persistent       │
                                  │  │  worker (trusted) │
                                  │  └─ sandboxed        │
                                  │     Rscript (codegen)│
                                  │  base + jsonlite +   │
                                  │  MASS, stats,        │
                                  │  KernSmooth          │
                                  └──────────────────────┘
```

Routing is **path-based** through a single proxy on port 80: `/` is served by `pd-dashboard`, `/api/...` by `api-server`. Services never call each other directly by port; the dashboard always uses relative `/api/...` URLs that go back through the proxy.

### 2.3 Tech stack [Eng]

| Layer | Choice | Notes |
| --- | --- | --- |
| Runtime | **Node 24** | ESM-only, native fetch, source maps in prod |
| Monorepo | **pnpm workspaces** | TS project references for libs (composite), `tsc --noEmit` for leaf packages |
| API framework | **Express 5** | Async error handling enabled |
| Validation | **Zod (`zod/v4`)** + **drizzle-zod** | Schemas generated from Drizzle tables |
| API contract | **OpenAPI 3.1** at `lib/api-spec/openapi.yaml` | Source of truth for all client codegen |
| Client codegen | **Orval** → React Query hooks (`lib/api-client-react`) + Zod schemas (`lib/api-zod`) | Run via `pnpm --filter @workspace/api-spec run codegen` |
| Database | **PostgreSQL** + **Drizzle ORM** | Schema-first, migrations checked in under `lib/db/drizzle/` |
| Build | **esbuild** | CJS bundle for the API; Vite for the SPA |
| Logging | **pino** + **pino-http** | Use `req.log` in handlers, singleton `logger` elsewhere — **never** `console.log` in server code |
| Computation | **R 4.5** + **jsonlite** | Trusted pipeline scripts run in a single long-lived R worker process (NDJSON request/response loop in `src/r-scripts/lib/worker-loop.R`); LLM-generated scripts run in isolated, resource-capped `Rscript` subprocesses (see §6.5) |
| LLM | **Anthropic `claude-sonnet-4-6`** | Reached through `lib/integrations-anthropic-ai`, which proxies to the Replit AI Integrations endpoint with prompt-caching enabled at all 4 call sites |
| Frontend | **React 18 + Vite + Tailwind + Radix + TanStack Query** | Generated hooks + Zod for data fetching |
| File storage | Local filesystem under `DATA_DIR` | Uploads are content-hashed; per-run dirs hold all artifacts |

### 2.4 Why this shape

* **R for numerics, JS for orchestration, LLM for judgement.** Splitting the responsibilities makes every numeric in the validation report independently reproducible from the R artifacts (RDS / RData) without re-running the LLM.
* **No model is silently overwritten.** Every run lives in its own run-id directory; nothing is mutated after the fact. Audit immutability is achieved by append-only DB inserts and content-hashed uploads.
* **Repair loop, not retry loop.** When the Model Developer rejects a handoff, the Data Analyzer attempts a single deterministic repair using a typed feedback report — never a free-form re-prompt. (See §3.5.)
* **One R worker for the trusted path.** Spawning `Rscript` per call costs ~2.5 s of startup just to load `jsonlite`. The api-server keeps one warm R process and dispatches each pipeline step over an NDJSON request/response loop, dropping warm-call overhead to single-digit milliseconds. Each request runs in a fresh child environment (`new.env(parent = globalenv())`) so top-level assignments do not leak across calls; library side-effects intentionally persist (that is what makes calls 2 and onward fast). `quit()/q()` from a script throws a typed `worker_quit` condition that the worker unwinds cleanly so early exits do not kill the worker. The worker is a process-global singleton with serialised requests, which is fine because the orchestrator runs steps sequentially within a run; a per-run worker pool with timeouts is a known follow-up. **Sandboxed codegen scripts do *not* use this worker** — they spawn through `r-sandbox.ts` because their isolation requirements are different.

---

## 3. The Multi-Agent PD Pipeline

### 3.1 Lifecycle overview

A run progresses through the following states, all persisted on `pipeline_runs.status`:

```
pending  →  running  →  completed
                    └─►  failed  (with `failureReason` set; see §3.6)
```

Inside `running`, `pipeline_runs.currentAgent` is updated to one of:
`data_analyzer`, `model_developer`, `model_validator`.

The orchestrator (`artifacts/api-server/src/agents/orchestrator.ts`) drives the state machine:

```
orchestrator.runPipeline(runId, csvPath, targetColumn, holdoutCsvPath?, opts?)
  │
  ├─► Data Analyzer  (Agent 1, 1st line)
  │     ├─ profiling     (data_quality.R)
  │     ├─ IV / WOE      (calculate_iv.R)
  │     ├─ imputation    (impute.R)              [conservative by default]
  │     ├─ LLM analysis  (claude-sonnet-4-6)
  │     └─ writeHandoffPackage(version=1)        → cleaned.csv + manifest
  │
  ├─► Model Developer (Agent 1, 1st line)
  │     ├─ feature pre-selection  (LLM)
  │     ├─ training loop          (train_model.R, GLM + challenger)
  │     ├─ candidate inspection   (LLM picks champion)
  │     │
  │     └─ rejection?  ──► FeedbackReport ──► Data Analyzer.repair()
  │                                              writeHandoffPackage(v2, extraSteps)
  │                                              ↑ one-shot only
  │
  └─► Model Validator (Agent 1, 2nd line)
        ├─ recalculate metrics on holdout (or in-sample)
        ├─ PSI per feature + on score (calculate_psi.R)
        ├─ LLM verdict                  (claude-sonnet-4-6)
        └─ summarise coefficient_diagnostics + psi_results before LLM call
```

Every state transition emits both an SSE event (live) and an `audit_log` row (persistent). Both flows are described in §3.7.

### 3.2 Agent 1 — Data Analyzer

**Source:** `artifacts/api-server/src/agents/data-analyzer/`

Composed of small modules:

| File | Responsibility |
| --- | --- |
| `index.ts` | Public entry point, threads the analyzer end-to-end |
| `config.ts` | `DataAnalyzerConfig` defaults + per-run overrides |
| `planner.ts` | Calls the script registry for each step, records selection log |
| `script-registry.ts` | Pluggable R-script registry + LLM codegen integration |
| `assessment.ts` | Builds the `DatasetAssessment` (retail segment, leakage rating) |
| `eligibility.ts` | Per-feature `EligibilityDecision` engine |
| `leakage.ts` | Keyword + pattern leakage detector |
| `inspector.ts` | Glues raw R output into typed `FeatureAssessment[]` |
| `imputation.ts` | Strategy chooser → calls `impute.R` |
| `handoff.ts` | Writes the v1 (or v2) handoff package and manifest |
| `repair.ts` | Deterministic repair handler (task #29) |
| `report.ts` | Renders `eda_report.md` |
| `prompts.ts` | LLM prompt templates with prompt-caching markers |
| `script-codegen.ts` | LLM-generated R script pipeline |
| `codegen-checks.ts` | Static-check pass for generated scripts |
| `persist-script.ts` | Writes generated scripts to DB + disk |
| `approved-scripts.ts` | Pre-approved generated scripts, registered at startup |
| `types.ts` | Public + internal types (no runtime code) |

**Outputs (typed `DataAnalyzerOutput`):**

* `dataset_assessment` — retail segment, dataset structure (application / behavioural panel / vintage cohort / ambiguous), data-quality / representativeness / leakage ratings, `overall_readiness` (`ready` / `ready_with_caveats` / `not_ready`).
* `approved_features`, `approved_with_restrictions`, `monitoring_only_features`, `unclear_features`, `excluded_features`.
* `feature_assessments` — per-feature decision + reason + IV + leakage level.
* `leakage_watchlist` — features the validator should watch.
* `handoff` — pointer to the v1/v2 package on disk (see §9.1).
* `script_selection_log` — every R script considered for every step (selected and rejected) with score + reason. **Audit critical.**
* `imputation_log` — per-column strategy applied, before/after missing %, fill value.
* `repair_history` — present only when a repair was triggered.
* `generated_scripts` — codegen records (when `enable_script_codegen` is on).

**Conservative defaults** (`config.ts`):

* `missingness_threshold_pct = 50` — drop a feature if more than half its values are missing.
* `dominant_value_threshold_pct = 95` — drop dominant-value features.
* `iv_min_threshold = 0.02`, `iv_strong_threshold = 0.3`, `iv_leakage_threshold = 0.5` (IV above this is suspicious — likely leakage).
* `imputation_policy = "conservative"` — median (numeric), mode (categorical), `drop_column` for high-missing.
* `enable_script_codegen = false` — opt-in per run via the `enableCodegen` request flag.
* `codegen_review_required = true` — generated scripts are held pending human review.

### 3.3 Agent 2 — Model Developer

**Source:** `artifacts/api-server/src/agents/model-developer.ts`

Steps:

1. **Feature pre-selection** — The LLM ranks the analyzer's `approved_features` and proposes a candidate subset. Anthropic prompt caching is enabled on the static system block.
2. **Training loop** — For each candidate model type (`logistic_regression`, `gradient_boosting` challenger), `train_model.R` is invoked with the cleaned CSV. Each candidate produces:
   * model coefficients + standard errors
   * AUC / Gini / KS on a 70/30 split (seed 42)
   * `coefficient_diagnostics` (VIF, p-values, sign flips)
   * an RDS model file and an RData reproducibility bundle
3. **Champion selection** — The LLM compares candidate metrics + diagnostics and picks the champion. The non-champion is preserved as the **challenger**.
4. **Rejection** — If every candidate fails to train, `mapTrainingErrorsToFeedback()` (`feedback.ts`) translates the R errors into a typed `FeedbackReport` for the orchestrator.

### 3.4 Agent 3 — Model Validator

**Source:** `artifacts/api-server/src/agents/model-validator.ts`

Independent 2nd-line review. It loads the model from disk (RDS), recomputes metrics from the cleaned dataset (and the holdout if attached), runs `calculate_psi.R`, and asks the LLM for a verdict.

Outputs:

* `recalculated_metrics` — independent AUC/Gini/KS.
* `psi_results` — feature-level PSI + score PSI (in-sample and out-of-sample if a holdout was attached).
* `coefficient_diagnostics` — restated to make the validation report self-contained.
* `validation_findings` — typed list with severity.
* `validation_status` — `pass` / `pass_with_conditions` / `fail`.
* `model_risk_rating` — `low` / `medium` / `high`.
* IRB-style approval recommendation.

Before the LLM call, `coefficient_diagnostics` and `psi_results` are **summarised** (largest |VIF|, top-N PSI moves, count of sign flips) so the prompt stays under the cache-friendly token budget while still surfacing the worst offenders.

### 3.5 The repair loop [Eng] [Audit]

If the Model Developer rejects all candidates, the orchestrator inspects the `FeedbackReport`:

```typescript
recoverable = report.issues.some(isRepairable)
            = some issue has code ∈ {wrong_dtype, too_many_nulls,
                                     invalid_categorical_encoding,
                                     feature_inconsistency,
                                     target_leakage_risk,
                                     broken_formatting}
              AND has a `column` field
```

If `recoverable === true`, the orchestrator calls `repairDataAnalyzer()` **once**. The repair handler:

1. Drops or recodes the column named in the feedback issue.
2. Re-emits the handoff package as **v2** with `extraSteps` appended to `transformation_log.steps` (each annotated `triggered_by: "model_developer"`, `issue_code`, `action_taken`).
3. Records a `RepairHistoryEntry` in `DataAnalyzerOutput.repair_history`.

The Model Developer is then re-invoked on the v2 package. If it rejects again, the run terminates with `failureReason = "modeller_rejected_after_repair"`. This intentionally caps the loop at one retry — repeated rounds would erode auditability without adding signal.

For testing, `forceModellerFeedbackForTest` on the start-run request injects a synthetic `FeedbackIssue[]` so the repair leg can be exercised without a genuinely-broken dataset (used by `scripts/smoke-repair.mjs`).

### 3.6 Failure taxonomy [Audit]

`pipeline_runs.failure_reason` carries a stable enum so dashboards and downstream consumers can branch on cause without parsing free-text logs:

| Value | Meaning |
| --- | --- |
| `data_analyzer_failed` | Agent 1 raised an unhandled exception |
| `modeller_rejected_unrepairable` | Feedback report had no repairable issue |
| `modeller_rejected_after_repair` | v2 handoff was still rejected |
| `model_developer_failed` | Agent 2 raised an unhandled exception |
| `model_validator_failed` | Agent 3 raised an unhandled exception |
| `repair_failed` | Repair handler itself threw |
| `orchestrator_error` | Any other orchestration-level error |

Forward-compatible: any other free-form value is tolerated (the dashboard falls back to displaying it verbatim).

### 3.7 Real-time event stream [Eng]

The orchestrator publishes events through `subscribeToRun(runId, listener)` (`run-events.ts`), which the SSE endpoint `GET /api/pipeline/runs/:runId/stream` exposes to the dashboard.

| Event type | When | Payload |
| --- | --- | --- |
| `current_state` | On SSE connect | The current `pipeline_runs` row |
| `agent_start` | An agent begins | `{ agent }` |
| `agent_progress` | Step inside an agent (e.g. R script complete) | `{ agent, step, detail }` |
| `agent_complete` | An agent finishes successfully | `{ agent, output }` |
| `agent_error` | An agent fails | `{ agent, error }` |
| `pipeline_complete` | All three agents succeeded | Final `pipeline_runs` row |
| `pipeline_failed` | Any unrecoverable failure | `{ failureReason }` |

Keep-alive `: keepalive` comments are sent every 15 s so intermediate proxies don't time out the stream.

---

## 4. REST API Reference

**Base URL.** All endpoints are under `/api` (path is rewritten by neither the proxy nor the framework — handlers see the full path).
**Spec.** The OpenAPI 3.1 contract lives at `lib/api-spec/openapi.yaml` and is the source of truth for codegen. Changes to handlers MUST be paired with spec changes (otherwise the generated client and Zod schemas drift).

### 4.1 Health

#### `GET /api/healthz`

Returns `{ status: "ok" }`. Used by Replit deploy health checks.

### 4.2 Uploads

#### `POST /api/pipeline/upload` (multipart/form-data)

Field | Type | Notes
--- | --- | ---
`file` | binary, ≤ 50 MiB | CSV
`targetColumn` | string (optional) | Hinted target

**Behaviour:**
* The file is streamed to disk under `<DATA_DIR>/uploads/tmp_<uuid>_<originalname>`.
* SHA-256 hashed (streaming, never fully in memory). The first 16 hex chars become the `fileId` prefix.
* If a file with the same hash already exists, the temp upload is unlinked and the existing `fileId` is returned (`deduplicated: true`).
* Otherwise the original filename is sanitised (`[^A-Za-z0-9._-] → _`; leading dots/underscores stripped; capped at 120 chars preserving extension) and renamed to `<hash16>_<safeOriginal>`.
* The CSV is streamed once to extract headers and row count without buffering.
* A best-effort target-column guess (`/default|target|pd|bad|churn|event/i`) is returned alongside.

**Response (`UploadResponse`)**: `{ fileId, fileName, rowCount, columnCount, columns, targetColumn, deduplicated? }`.

#### `GET /api/pipeline/uploads`

Lists every stored upload joined to pipeline-run usage:

```json
{
  "uploads": [{
    "fileId": "ab12...",
    "fileName": "loans.csv",
    "sizeBytes": 134217,
    "uploadedAt": "2026-05-02T...",
    "runCount": 4,
    "activeRunCount": 0,
    "isSample": false
  }]
}
```

`activeRunCount` includes uses as either `fileId` *or* `holdoutFileId`. In-flight `tmp_*` files are filtered out.

#### `DELETE /api/pipeline/uploads/:fileId`

Refuses to delete:
* the bundled sample dataset (`sample_credit_data.csv`) — 400;
* `fileId`s outside the strict regex `^[A-Za-z0-9._-]+$` or with traversal attempts — 400;
* files referenced by any run currently `pending` or `running` (training **or** holdout) — 409 with `activeRunCount`.

Defence-in-depth: even after the regex check, the resolved path must remain inside `UPLOADS_DIR` (`path.resolve` boundary check).

#### `GET /api/pipeline/sample-data`

Returns metadata for the bundled sample dataset, generating it on demand if missing. The generator simulates 1,000 borrowers across 12 columns with a plausible default distribution (driven by credit score, debt ratio, delinquencies, employment, income).

### 4.3 Runs

#### `POST /api/pipeline/runs`

Request (`StartRunRequest`):

```json
{
  "fileId": "ab12_loans.csv",
  "targetColumn": "default",
  "runName": "Q3 retail PD",
  "holdoutFileId": "cd34_loans_q4.csv",
  "enableCodegen": false,
  "codegenRequireReview": true,
  "forceModellerFeedbackForTest": [
    { "code": "wrong_dtype", "column": "income", "severity": "blocking", "details": "..." }
  ]
}
```

* `fileId` and `targetColumn` are required.
* `holdoutFileId` must differ from `fileId` (PSI of a sample against itself is ~0; we reject explicitly to prevent a misleading "stable OOS" verdict).
* `enableCodegen` and `codegenRequireReview` are surfaced into `DataAnalyzerConfig` for this run.
* `forceModellerFeedbackForTest` is silently dropped if malformed; valid entries inject a synthetic feedback report on the modeller's first attempt for repair-loop testing.

Behaviour: inserts a `pipeline_runs` row in `pending`, creates three `agent_results` placeholder rows, then kicks off `runPipeline(...)` asynchronously and returns `{ runId, status: "running" }` immediately.

#### `GET /api/pipeline/runs`

Lists the 50 most recent runs, ordered by `started_at DESC`.

#### `GET /api/pipeline/runs/:runId`

Returns the full `PipelineRunDetail`:

```json
{
  "id": "...", "name": "...", "status": "completed",
  "currentAgent": null, "fileId": "...", "targetColumn": "default",
  "holdoutFileId": "...",
  "startedAt": "...", "completedAt": "...",
  "validationStatus": "pass_with_conditions",
  "modelRiskRating": "medium",
  "failureReason": null,
  "repairHistory": [/* lifted from data_analyzer.output.repair_history */],
  "agentResults": [
    { "agentName": "data_analyzer", "status": "completed",
      "startedAt": "...", "completedAt": "...",
      "output": { /* DataAnalyzerOutput */ }, "hasReport": true },
    /* model_developer, model_validator */
  ],
  "auditLog": [ { "id", "timestamp", "agent", "action", "details", "actor" } ]
}
```

`auditLog` is capped at 100 entries (in `timestamp ASC` order). Larger trails are downloadable as CSV via §4.5.

#### `GET /api/pipeline/runs/:runId/stream` (SSE)

Streams the events listed in §3.7. Sends `current_state` immediately on connect, then `: keepalive` every 15 s. The handler unsubscribes on `req.close`.

### 4.4 Reports & artifacts

#### `GET /api/pipeline/runs/:runId/report/:agentName`

Returns `{ agentName, content }` where `content` is the markdown of one of:
* `data_analyzer` → `eda_report.md`
* `model_developer` → `dev_report.md`
* `model_validator` → `validation_report.md`

#### `GET /api/pipeline/runs/:runId/report/:agentName/download`

Same content with `Content-Disposition: attachment; filename="RiskCore_<Label>_<shortId>.md"`. Labels: `EDA_Report`, `Model_Development_Report`, `Validation_Report`.

#### `GET /api/pipeline/runs/:runId/handoff/:filename`

Streams a file from the v1 handoff package. `:filename` must be one of `cleaned.csv`, `schema.json`, `data_quality_summary.json`, `transformation_log.json`, `handoff_manifest.json`. `runId` must match `^[a-zA-Z0-9_-]+$`. The path resolves to `<DATA_DIR>/<runId>/data-analyzer/v1/<filename>`.

#### `GET /api/pipeline/runs/:runId/model/:modelType`

Streams the trained R model (`.rds`). `modelType` is allow-listed against the `candidate_models[].model_type` values found in the run's Model Developer output, so an attacker cannot list arbitrary `.rds` files in the run dir.

#### `GET /api/pipeline/runs/:runId/model/:modelType/rdata`

Streams the full reproducibility bundle (`.RData`) written by `train_model.R`. The bundle packages the input dataframe, the cleaned modelling frame, train/test splits, the fitted GLM, and the feature/target/seed metadata so a reviewer can `load()` it in R and reproduce every result without chasing CSVs and RDS files separately.

#### `GET /api/pipeline/runs/:runId/journal/download`

Returns a UTF-8 CSV (with BOM, CRLF) containing one row per audit-log entry plus enrichment columns derived from the agent outputs. Filename: `RiskCore_LogJournal_<runId>.csv`.

Columns: `timestamp`, `step_number`, `agent`, `action`, `r_script`, `input_summary`, `output_summary`, `llm_model`, `llm_decision`, `features_selected`, `metrics_auc`, `metrics_gini`, `metrics_ks`, `validation_status`, `actor`, `details`.

This is the one-file evidence pack a regulator typically asks for: every step, the R script that ran, what it consumed, what it produced, the LLM decision (if any) with model name, the metrics at that point in the run, and the human actor when one was involved (e.g. script approvals).

### 4.5 Generated scripts (codegen review queue)

#### `GET /api/pipeline/runs/:runId/scripts`

Returns every codegen attempt for the run with its current review state. Each entry (`GeneratedScript`):

```json
{
  "scriptId": "gen_iv_3f2a8c",
  "runId": "...", "agentName": "data_analyzer", "step": "iv",
  "status": "pending_review",
  "producedAt": "...", "reviewedAt": null,
  "reviewerName": null, "reviewNotes": null, "rejectedReason": null,
  "prompt": "<full LLM prompt>",
  "sourceCode": "<full R source>",
  "scriptPath": "/data/<runId>/codegen/iv/gen_iv_3f2a8c.R",
  "spec": { "purpose": "...", "step": "iv", "inputs": [...], "outputs": [...] },
  "staticCheck": { "ok": true, "violations": [], "allowed_libraries": ["jsonlite"] },
  "dryRun": { "ok": true, "exit_code": 0, "duration_ms": 412, ... }
}
```

`status` is one of: `codegen_rejected` (failed static check or dry-run), `registered` (auto-registered, no review required), `pending_review` (awaiting approval), `approved`, `rejected`.

#### `POST /api/pipeline/runs/:runId/scripts/:scriptId/approve`

Body: `{ reviewer: string, notes?: string }`. Atomically transitions a `pending_review` row to `approved`, recording reviewer, ISO timestamp, and notes. Concurrent decisions are rejected with 409. A `codegen_script_approved` audit-log entry is appended with the reviewer as `actor`.

#### `POST /api/pipeline/runs/:runId/scripts/:scriptId/reject`

Body: `{ reviewer: string, reason?: string }`. Same atomic transition to `rejected`; emits `codegen_script_rejected`.

### 4.6 Error envelope

All non-2xx JSON responses use:

```json
{ "error": "Human-readable message", "details": "Optional secondary detail" }
```

`409 Conflict` from `DELETE /uploads/:fileId` additionally carries `activeRunCount: number`.

---

## 5. Database Schema

### 5.1 `pipeline_runs`

The lifecycle row for one PD run. All other tables foreign-key (logically) on `id`.

| Column | Type | Notes |
| --- | --- | --- |
| `id` | text PK | uuid v4 |
| `name` | text | Display name |
| `status` | text NOT NULL DEFAULT 'pending' | `pending` / `running` / `completed` / `failed` |
| `current_agent` | text | Set during `running` |
| `file_id` | text NOT NULL | Training dataset upload id |
| `target_column` | text NOT NULL | Header name in the CSV |
| `holdout_file_id` | text | Optional OOS dataset id |
| `started_at` | timestamp NOT NULL DEFAULT now() | |
| `completed_at` | timestamp | Set on success or failure |
| `validation_status` | text | Final verdict (`pass`/`pass_with_conditions`/`fail`) |
| `model_risk_rating` | text | `low` / `medium` / `high` |
| `failure_reason` | text | Stable enum, see §3.6 |

### 5.2 `agent_results`

One row per (run, agent). Inserted as `pending` placeholders on run creation, then promoted by the orchestrator.

| Column | Type | Notes |
| --- | --- | --- |
| `id` | text PK | |
| `run_id` | text NOT NULL | Indexed: `agent_results_run_id_idx` |
| `agent_name` | text NOT NULL | `data_analyzer` / `model_developer` / `model_validator` |
| `status` | text NOT NULL DEFAULT 'pending' | `pending` / `running` / `completed` / `failed` |
| `started_at`, `completed_at` | timestamp | |
| `output` | jsonb | Typed: `DataAnalyzerOutput`, `ModelDeveloperOutput`, `ModelValidatorOutput` |
| `has_report` | integer NOT NULL DEFAULT 0 | 1 when the markdown report file exists |

### 5.3 `audit_log` (append-only)

The single source of truth for "what happened, when, who decided what". Indexed by `(run_id, timestamp)` so the dashboard can stream a run's history with one index seek.

| Column | Type | Notes |
| --- | --- | --- |
| `id` | text PK | |
| `run_id` | text NOT NULL | Composite-indexed |
| `agent` | text NOT NULL | Agent that emitted the entry, or a system label |
| `action` | text NOT NULL | Stable verb (`pipeline_started`, `eda_complete`, `iv_complete`, `lr_trained`, `gb_trained`, `psi_complete`, `llm_analysis_complete`, `llm_validation_complete`, `model_selected`, `pipeline_completed`, `pipeline_failed`, `codegen_script_approved`, `codegen_script_rejected`, …) |
| `details` | text NOT NULL | Free-form prose. LLM rationales land here. |
| `actor` | text | Human reviewer when relevant; null for agent-emitted rows |
| `timestamp` | timestamp NOT NULL DEFAULT now() | |

**Index:** `audit_log_run_id_timestamp_idx` on `(run_id, timestamp)`.

### 5.4 `generated_scripts`

Holds every LLM-generated R script attempt (whether it ever ran or not), with full review state. Unique `(run_id, script_id)`.

| Column | Type | Notes |
| --- | --- | --- |
| `id` | text PK | |
| `run_id` | text NOT NULL | |
| `agent_name` | text NOT NULL | Always `data_analyzer` today |
| `step` | text NOT NULL | `profiling` / `iv` / `psi` / `imputation` / `training` / `custom` |
| `script_id` | text NOT NULL | Stable id (e.g. `gen_iv_3f2a8c`) |
| `script_path` | text | Absolute path on disk; null if not persisted |
| `prompt` | text NOT NULL | Full prompt sent to Claude |
| `source_code` | text NOT NULL | Full R source |
| `spec` | jsonb NOT NULL | `GeneratedScriptSpec` (purpose, step, inputs, outputs) |
| `static_check` | jsonb NOT NULL | `StaticCheckResult` (ok, violations, allowed_libraries) |
| `dry_run` | jsonb | `DryRunResult` or null if static check failed first |
| `status` | text NOT NULL | `codegen_rejected` / `registered` / `pending_review` / `approved` / `rejected` |
| `rejected_reason` | text | Set when `status = codegen_rejected` |
| `produced_at` | timestamp NOT NULL DEFAULT now() | |
| `reviewed_at`, `reviewer_name`, `review_notes` | | Set by approve/reject |

**Index:** `generated_scripts_run_script_uq` UNIQUE on `(run_id, script_id)`.

### 5.5 Migrations [Eng]

Drizzle Kit drives schema management.

* Schema TS source: `lib/db/src/schema/*`.
* Generated SQL: `lib/db/drizzle/0000_*.sql` (checked in; reproducible).
* Apply against current `DATABASE_URL`: `pnpm --filter @workspace/db run push`.

There is currently a single baseline migration. New schema changes should:
1. Edit the TS schema.
2. Generate a new Drizzle migration into `lib/db/drizzle/` (do **not** hand-edit existing files).
3. Run `push` against dev, then commit the new migration before merging.

For production, applying migrations is a manual step performed against the production database via the Replit database tooling — see §8.4.

---

## 6. R Script Registry & LLM Codegen

### 6.1 Why a registry?

The analyzer used to call `runRScript("data_quality.R", ...)` directly at each step. As the catalogue grew (and as task #30 added LLM-generated scripts), the call sites accumulated branching logic about which script handled which step, what arguments it expected, and how to log the choice.

The **script registry** (`script-registry.ts`) inverts that: every R script registers itself with a `canHandle(ctx)` function that returns `{ score, reason }`, and the registry picks the highest-scoring entry per step. The result is:

* **Pluggable** — adding a new script is a single `register()` call.
* **Auditable** — every candidate considered is logged with score + reason in `script_selection_log`, even if it lost.
* **Codegen-ready** — LLM-generated scripts are just registry entries with a sandboxed executor and a higher score.

### 6.2 Default registered scripts

`createDefaultScriptRegistry(dataDir)` ships with:

| `id` | `path` | Step | Score | Inputs (CLI positional) | Outputs |
| --- | --- | --- | --- | --- | --- |
| `data_quality` | `data_quality.R` | `profiling` | 100 | csv, target, dataDir | `data_quality_issues`, `target_integrity_issues`, `feature_meta`, `feature_recommendations`, `dataset_structure`, `detected_columns` |
| `calculate_iv` | `calculate_iv.R` | `iv` | 100 (when `featureCount > 0`) | csv, target, comma-joined features, optional obsDate | `iv`, `woe`, `monotonicity`, `stability` |
| `calculate_psi` | `calculate_psi.R` | `psi` | 100 (+holdout bonus reserved) | refCsv, csv, optional target | `feature_psi`, `score_psi`, `sensitivity_analysis` |
| `impute` | `impute.R` | `imputation` | 100 | csv, planJson, outCsv | `imputed_csv`, `imputation_log` |
| `train_model` | `train_model.R` | `training` | 100 | csv, target, comma-joined features | `model_coefficients`, `diagnostics`, `rds_path` |

Plus the bootstrap helpers `install_packages.R` and the support library under `r-scripts/lib/`.

Default scripts return `score: 0` for any step they don't handle. The registry picks the highest scorer; ties go to the most recently registered entry.

### 6.3 Codegen flow [Eng] [Audit]

When `enable_script_codegen = true` and the best existing registered entry scores `≤ codegen_score_threshold` (default `80`, against the default scripts' `100`, so codegen rarely fires for the standard steps), `runForWithCodegen()` invokes Claude via `script-codegen.ts` to produce a new script.

The pipeline is uniformly defensive:

```
spec ──► generateAndValidateScript()
          │
          ├─ Anthropic prompt (system + few-shot, prompt-cached)
          │
          ├─ Static check   (codegen-checks.ts — see §6.4)
          │
          ├─ Sandboxed dry-run on a bundled fixture CSV
          │     (r-sandbox.ts: 30 s wall-clock, 512 MB R_MAX_VSIZE,
          │      ≥ 2 GB AS-cap via prlimit, cwd pinned to sandbox dir)
          │
          └─► GeneratedScriptRecord
                ├─ static_check_result
                ├─ dry_run_result
                ├─ pending_review (when codegen_review_required)
                └─ persisted to generated_scripts + disk
```

If both checks pass and review is **not** required, the entry is auto-registered into the script registry **for the current run** with score `200` (so it outranks the defaults). If review **is** required (the default), the entry is held in `pending_review` and the current step is skipped — reviewers approve via the dashboard and the script becomes available for subsequent runs.

### 6.4 Static-check pass [Audit]

`codegen-checks.ts` runs an exhaustive, deny-by-default scan over the comment-stripped, string-blanked source. The intent is **not** to be a full sandbox (that's `r-sandbox.ts`) but to refuse to ever execute a script that even *contains* a primitive capable of escaping the sandbox.

**Allow-listed packages:** `jsonlite`, `MASS`, `stats`, `KernSmooth`. Anything else loaded via `library()` is rejected.

**Forbidden primitives** (non-exhaustive — see source for the full list):

* Shell: `system`, `system2`.
* Env: `Sys.setenv`, `Sys.getenv`.
* Network: `download.file`, `url()`, anything from `httr` / `curl` / `RCurl` / `httr2`.
* Code-loading: `source`, `sys.source`, `parse`, `str2lang`, `str2expression`, `deparse`, `as.function`, `as.expression`, `as.name`, `as.symbol`.
* Eval family: `eval`, `evalq`, `eval.parent`, `local`, `Recall`.
* Indirect dispatch: `do.call`, `call`, `as.call`, `quote`, `bquote`, `substitute`; `Reduce`/`Filter`/`Find`/`Position`/`Map` with a string-named function; backticked invocation `` `name`(...) ``.
* Symbol/binding mutation: `get`, `mget`, `get0`, `getFunction`, `match.fun`, `assign`, `delayedAssign`, `makeActiveBinding`, `autoload`, `autoloader`.
* Environment graph: `baseenv`, `globalenv`, `emptyenv`, `topenv`, `environment`, `new.env`, `parent.frame`, `parent.env`, `sys.frame`/`sys.frames`/`sys.call`/`sys.calls`/`sys.function`/`match.call`.
* Namespaces: `getNamespace`, `asNamespace`, `loadNamespace`, `attachNamespace`, `attach`, `detach`, `getExportedValue`, `getFromNamespace`, `pkg:::sym` syntax.
* C-level: `.Internal`, `.Primitive`.
* Method tables: `getAnywhere`, `getS3method`, `getMethod`, `selectMethod`, `findMethod`, `existsMethod`, `hasMethod`, `slot`, `setMethod`/`setGeneric`/`setClass`/`setRefClass`/`R6Class`, `UseMethod`, `NextMethod`, `standardGeneric`, `callNextMethod`.
* AST mutation: `body()`, `formals()`, and the replacement forms `body<-`, `formals<-`.
* Persistence: `serialize`, `unserialize`, `saveRDS`, `save`, `load`, `readRDS` (sensitive-reader scan).
* Tracing/hooks: `trace`, `untrace`, `tracingState`, on.exit hooks installing arbitrary callables.

Each forbidden pattern is matched against a string-blanked, comment-stripped form of the source so a benign string literal containing the word `system` does not trigger a false positive. Namespace prefixes (`base::`, `methods::`, `utils::`, `grDevices::`) are tolerated as part of the match so the obvious bypass `base::system(...)` is closed.

A second async layer (`astCheckRScript`) spawns an Rscript helper to parse the source into an actual call tree and walks it with the same deny list, catching aliases like `f <- system; f()` and value-passing like `lapply(list(p), system)` that the regex pass cannot.

The combined `staticCheckRScriptFull` is what production codegen runs.

### 6.5 Sandboxed runner [Eng]

`runRScriptSandboxed(scriptAbsPath, args, opts)` (in `r-sandbox.ts`):

* **Wall-clock timeout** (default 30 s): `SIGTERM`, then `SIGKILL` 2 s later.
* **R-vector cap**: `R_MAX_VSIZE` env var, default 512 MB. Caps R's vector allocation precisely.
* **Address-space cap**: `prlimit --as=<bytes>` with floor `MIN_AS_CAP_MB = 2048`. Defence-in-depth ceiling on total VAS. The floor exists because R reserves ~1 GB at startup just to load shared libs and `jsonlite`; a tight 512 MB AS cap would kill `Rscript` before the script runs. Falls back gracefully when `prlimit` is not available (e.g. macOS dev).
* **cwd pinned** to the sandbox directory (default `path.dirname(scriptAbsPath)`) so relative-path reads/writes can only ever land inside it.
* **Output contract**: `success` is true only when the process exits 0 *and* `extractLastJson(stdout)` finds a complete JSON object. Otherwise `killed_by` is `"timeout"` or `"memory"` (inferred from `SIGSEGV` or `cannot allocate vector`).
* **Tail capture**: 4 KiB tails of stdout/stderr are returned for diagnostics without flooding logs.

### 6.6 Review queue lifecycle [Audit]

```
codegen produced ─► static check ─► dry-run ─► review-required?
                       │              │           │
                       ▼ fail         ▼ fail      ▼
                 codegen_rejected   codegen_     pending_review ─► approved
                                    rejected         │              │
                                                     ▼              ▼
                                                  rejected     (registered for
                                                               subsequent runs)
```

Every transition emits an `audit_log` entry. Approvals/rejections always carry the human reviewer in the `actor` column and (where given) review notes / rejection reason in `details`. `pending_review → approved | rejected` transitions are atomic and reject concurrent decisions with HTTP 409.

---

## 7. Frontend — `pd-dashboard`

### 7.1 Stack

* **React 18** + **TypeScript 5.9**.
* **Vite** with the Replit Cartographer / dev banner / runtime error modal plugins.
* **Tailwind 3** + **Radix UI** primitives + the `@tailwindcss/typography` plugin for rendering markdown reports.
* **TanStack Query** for server state, fed by the **generated** React Query hooks in `lib/api-client-react`.
* **Zod** schemas from `lib/api-zod` for runtime validation of API responses.
* **`react-hook-form`** for the new-run form.

The SPA serves at the path-routed prefix `/` (port 21706 in dev; the Vite build output under `dist/public` is served statically in production with a single `from = "/*", to = "/index.html"` rewrite for SPA routes).

### 7.2 Pages

`artifacts/pd-dashboard/src/pages/`:

| Page | Route | Purpose |
| --- | --- | --- |
| `dashboard.tsx` | `/` | Run list (50 most recent), upload list, "start run" CTA |
| `new-run.tsx` | `/new-run` | Upload picker → target column picker → optional holdout → codegen toggles → start run |
| `run-detail.tsx` | `/runs/:runId` | Live run view with SSE stream, agent cards, audit log, downloads, codegen review queue |
| `not-found.tsx` | `*` | 404 |

### 7.3 Real-time updates [Eng]

`run-detail.tsx` opens an `EventSource` against `/api/pipeline/runs/:runId/stream` on mount and merges incoming events into a local React state. On every `agent_complete` / `pipeline_complete` it invalidates the relevant TanStack Query caches so the rest of the page re-fetches in step.

### 7.4 Reviewing generated scripts [Audit]

When a run was started with `enableCodegen: true, codegenRequireReview: true`, the run-detail page surfaces a **Generated Scripts** card listing every entry returned by `GET /runs/:runId/scripts`. Each `pending_review` entry shows:
* the spec (purpose, step, inputs, outputs);
* the full prompt sent to Claude;
* the static-check result with violations (when any);
* the dry-run result (exit code, duration, parsed output, stdout/stderr tails);
* **Approve** / **Reject** buttons that prompt for the reviewer name and optional notes/reason.

After a decision, the entry's status chip flips to `approved` / `rejected` and the audit log gains a row attributed to the reviewer.

### 7.5 Running locally [Eng]

```bash
# Start everything via the per-artifact dev workflows configured in the repo.
# Do not run `pnpm dev` at the repo root — there is no root dev script by design.
pnpm --filter @workspace/api-server      run dev   # http://localhost:80/api
pnpm --filter @workspace/pd-dashboard    run dev   # http://localhost:80/
pnpm --filter @workspace/mockup-sandbox  run dev   # http://localhost:80/__mockup
```

In Replit the three workflows ship pre-configured (`artifacts/<name>: <label>`) and the proxy on port 80 routes `/`, `/api`, and `/__mockup` to the right backend.

---

## 8. Deployment & Operations Runbook

### 8.1 Environment variables [Eng] [Audit]

| Variable | Required | Used by | Notes |
| --- | --- | --- | --- |
| `DATABASE_URL` | yes | `lib/db` (Drizzle) | Postgres connection string. Created automatically by Replit's database provisioning. |
| `SESSION_SECRET` | yes | `api-server` | Cookie/session signing. Pre-provisioned. |
| `AI_INTEGRATIONS_ANTHROPIC_BASE_URL` | yes | `lib/integrations-anthropic-ai` | Replit AI Integrations proxy URL. The library throws at import time if missing. |
| `AI_INTEGRATIONS_ANTHROPIC_API_KEY` | yes | `lib/integrations-anthropic-ai` | Proxy API key. |
| `PORT` | injected | both artifacts | Wired by the artifact services (`8080` for api-server; `21706` for the dashboard in dev, static in prod). |
| `BASE_PATH` | injected | dashboard | Path prefix for SPA URLs (`/`). |
| `NODE_ENV` | yes (`production`) | logger, build | Controls log formatting and prod build path. |
| `LOG_LEVEL` | optional | `logger.ts` | Defaults to `info`. |
| `R_LIBS_USER` | optional | r-runner / r-sandbox | Defaults to `/home/runner/.R/library` (the location used by `install_packages.R`). |

**Never** `console.log` secrets and never read or print `AI_INTEGRATIONS_ANTHROPIC_API_KEY` for debugging — the audit log captures everything we need without it.

### 8.2 First-time setup [Eng]

1. Provision the **Anthropic AI integration** in the Replit workspace (it sets the two `AI_INTEGRATIONS_ANTHROPIC_*` vars).
2. Provision a **PostgreSQL database** (sets `DATABASE_URL`).
3. Install R packages once with `Rscript artifacts/api-server/src/r-scripts/install_packages.R`. This populates `R_LIBS_USER` with `jsonlite`, `MASS`, `stats`, `KernSmooth`. Subsequent restarts skip this step.
4. Apply migrations: `pnpm --filter @workspace/db run push`.
5. Start the workflows (or click "Run" on the Replit project).

### 8.3 Deployment [Eng]

The `api-server` artifact's `artifact.toml` declares production build/run commands:

```toml
[services.production.build]
args = ["pnpm", "--filter", "@workspace/api-server", "run", "build"]

[services.production.run]
args = ["node", "--enable-source-maps", "artifacts/api-server/dist/index.mjs"]
env = { PORT = "8080", NODE_ENV = "production" }

[services.production.health.startup]
path = "/api/healthz"
```

The dashboard ships as **static files**:

```toml
[services.production]
build = ["pnpm", "--filter", "@workspace/pd-dashboard", "run", "build"]
publicDir = "artifacts/pd-dashboard/dist/public"
serve = "static"

[[services.production.rewrites]]
from = "/*"
to = "/index.html"
```

Production publishing is performed via Replit's deploy flow (Reserved VM or Autoscale). The platform handles TLS, custom domains, and zero-downtime restarts. After deploy:

* The HTTPS URL is in `$REPLIT_DOMAINS` (comma-separated).
* `/api/healthz` is the readiness probe.

### 8.4 Production database changes [Eng]

Schema changes follow the dev workflow in §5.5. To apply migrations to production:

* Use the Replit production-database tooling (path-routed `production` environment) to run the generated SQL from `lib/db/drizzle/0000_*.sql` (and any new ones).
* **Never** edit a migration file that has been merged. Add a new one.
* Read-only debugging of production data should go through the `database` skill with `environment: "production"`.

### 8.5 Operational checklist

* **Restart cadence.** A workflow restart is required after editing TS source. Use `restart_workflow "artifacts/api-server: API Server"`. Edits to **trusted pipeline R scripts** (`data_quality.R`, `calculate_iv.R`, `calculate_psi.R`, `train_model.R`, `impute.R`, anything under `r-scripts/lib/`) are picked up on the **next** worker request because each call `source()`s the script fresh inside an isolated child environment — no restart needed. Edits to `worker-loop.R` itself **do** require an api-server restart, since the worker is long-lived. Sandboxed codegen scripts are spawned per call and are always picked up immediately.
* **Disk hygiene.** Each run writes to `<DATA_DIR>/<runId>/`. There is no automatic GC. Periodically prune `data/` for completed runs older than the retention window (regulatory minimum: 5 years for retail PD; check your jurisdiction).
* **Upload retention.** `DELETE /api/pipeline/uploads/:fileId` is the supported way to remove raw datasets (see §4.2 for refusal conditions).
* **Backups.** Point-in-time backups of Postgres are managed by the Replit database service. The on-disk artifacts under `data/` are **not** backed up automatically — if you need durable evidence, copy run dirs to object storage on completion (out of scope for this build).

### 8.6 Smoke tests [Eng]

Three Node-driven smoke harnesses exist under `artifacts/api-server/scripts/`:

| Script | Command | Exercises |
| --- | --- | --- |
| `smoke-handoff.mjs` | `pnpm --filter @workspace/api-server run smoke:handoff` | Data analyzer end-to-end on the bundled sample, asserts manifest fields and SHA invariants |
| `smoke-repair.mjs` | `pnpm --filter @workspace/api-server run smoke:repair` | Forces a synthetic feedback report, asserts repair leg writes v2 manifest and `repair_history` |
| `smoke-codegen.mjs` / `smoke-codegen-e2e.mjs` | `pnpm --filter @workspace/api-server run smoke:codegen[-e2e]` | Codegen happy path + sandbox + review queue transitions |

Run all three before publishing a release. The most recent prior change (Anthropic prompt caching + diagnostics summarisation) was validated by `smoke:handoff` reporting 8/8.

### 8.7 Triage map

| Symptom | First place to look |
| --- | --- |
| Run stuck in `pending` | `/api/pipeline/runs/:id` → `agentResults[].status`; check api-server logs for orchestrator exceptions |
| `failed` with no obvious cause | `pipeline_runs.failure_reason` (§3.6) + last `audit_log` entries |
| `pipeline_failed` SSE but no DB row | Check `req.log.error` lines from `routes/pipeline/index.ts` |
| Generated script never registered | `generated_scripts.status` and `static_check.violations` |
| Missing `cleaned.csv` on dashboard | `data-analyzer/v1/handoff_manifest.json` exists? `manifest_self_sha_note` indicates the package was rewritten correctly |
| R subprocess timeout (sandboxed codegen) | `r-sandbox.ts` `killed_by` → `timeout` / `memory`; bump `timeoutMs` / `maxMemMB` in the codegen spec |
| Trusted pipeline step hangs / api-server unresponsive | The persistent R worker (`r-runner.ts` + `worker-loop.R`) serialises requests; a hung trusted script blocks subsequent steps. Restart `artifacts/api-server: API Server` to respawn the worker. In-flight requests reject with the worker death; the orchestrator surfaces this as the run's `failureReason`. |
| Pipeline step succeeds but state leaks across runs | Should not happen — each request runs in a fresh `new.env(parent = globalenv())`. If suspected, check `worker-loop.R` for accidental `<<-` or `assign(envir = globalenv())` in a recently-edited trusted script. |
| Anthropic 401 / 5xx | Verify both `AI_INTEGRATIONS_ANTHROPIC_*` env vars are set and the integration is provisioned |
| Migrations not applied | `pnpm --filter @workspace/db run push`; for production use the production-environment tooling |

---

## 9. Data Contracts

This section documents every artifact that crosses an agent boundary or is exposed to a regulator. All file contracts are versioned (`version` integer) and re-emitted only by appending a new `version` — files are never mutated in place.

### 9.1 Handoff package

Location: `<DATA_DIR>/<runId>/data-analyzer/v<version>/`.

```
v1/
├── cleaned.csv                      # post-cleaning, post-imputation modelling input
├── schema.json                      # column roles + business domain + decisions
├── data_quality_summary.json        # row/column counts, dropped, imputation summary
├── transformation_log.json          # ordered list of every step that ran
└── handoff_manifest.json            # canonical pointer file; lists self under artifacts[]
```

* `version: 1` is the original analyzer output; `version: 2+` is the result of one or more deterministic repairs.
* `cleaned_csv_sha256` in the manifest is always the digest of whatever bytes are on disk (post-imputation if the engine ran).
* The manifest lists itself under `artifacts[]`. To break the chicken-and-egg, the SHA recorded in the self-entry is the digest of the manifest **without** the self-entry. This is documented inline in the file via `manifest_self_sha_note`.
* **Hard invariants** (`writeHandoffPackage` refuses to emit a misleading package if violated):
  * the target column must be present in the source CSV;
  * at least one feature column must remain after cleaning.

### 9.2 `transformation_log.json`

```json
{
  "version": 2,
  "generated_at": "2026-05-02T14:31:00.000Z",
  "source_file_id": "ab12_loans.csv",
  "source_csv_path": "/data/uploads/ab12_loans.csv",
  "target_column": "default",
  "steps": [
    { "step": 1, "name": "column_selection", "detail": "Kept 11 of 22 ..." },
    { "step": 2, "name": "row_cleaning",     "detail": "Dropped 3 of 1000 ..." },
    { "step": 3, "name": "imputation",       "detail": "Imputation engine: 4/8 ..." },
    { "step": 4, "name": "repair_drop_column",
      "detail": "Dropped 'income_text' because Modeller flagged wrong_dtype.",
      "triggered_by": "model_developer", "issue_code": "wrong_dtype",
      "action_taken": "drop_column" }
  ],
  "columns_kept": ["age","income","utilisation","default"],
  "columns_dropped": [{ "name": "name", "reason": "Excluded by analyzer." }],
  "rows_dropped":    [{ "row_index": 17, "reason": "All kept columns blank" }],
  "total_rows_in": 1000, "total_rows_out": 997,
  "imputation": [
    { "column": "income", "strategy": "median", "reason": "...",
      "applied": true, "before_missing_pct": 12.4, "after_missing_pct": 0,
      "rows_filled": 124, "fill_value": 48000, "indicator_column_added": false }
  ]
}
```

### 9.3 `schema.json`

Per-column role, data type, business domain, and the analyzer's decision. `role` is one of `target`, `feature`, `feature_restricted`, `monitoring`, `missingness_indicator`. Indicator columns appear only when the imputation engine added them and `add_missingness_indicator` was true.

### 9.4 `data_quality_summary.json`

Aggregates: source vs cleaned row/column counts, columns dropped, columns missing from source, EDA dataset summary, `imputation_summary` (engine_ran, columns_imputed, columns_dropped, rows_dropped), and a per-feature snapshot (decision, missing rate, dominant value rate, IV, leakage risk level).

### 9.5 `FeedbackReport` (Modeller → Analyzer)

```typescript
interface FeedbackReport {
  run_id: string;
  attempt_no: 1 | 2;
  rejected_by: "model_developer" | "model_validator";
  issues: FeedbackIssue[];
  recoverable: boolean;     // true iff some issue has a deterministic repair
  summary: string;
  emitted_at: string;       // ISO-8601
}

interface FeedbackIssue {
  code: "unsupported_schema" | "wrong_dtype" | "too_many_nulls" |
        "invalid_categorical_encoding" | "feature_inconsistency" |
        "target_leakage_risk" | "broken_formatting" | "missing_required_field";
  column?: string;          // column-scoped issues set this
  severity: "warning" | "blocking";
  details: string;
  source_error?: string;    // verbatim R error for traceability
}
```

The contract is dependency-free of agent internals so OpenAPI codegen can include it.

### 9.6 `RepairHistoryEntry` (Analyzer self-record)

```typescript
interface RepairHistoryEntry {
  attempt_no: number;
  triggered_by: "model_developer" | "model_validator";
  issue_code: FeedbackIssueCode;
  column: string | null;
  action_taken: string;     // human-readable description
  details: string;          // contextual narrative
}
```

### 9.7 R script outputs

Every R script writes a single JSON object to stdout. The runner captures only the **last complete JSON block** (so non-fatal R warnings printed before don't break parsing). The contracts:

* `data_quality.R` → `{ data_quality_issues, target_integrity_issues, feature_meta, feature_recommendations, dataset_structure, detected_columns }`
* `calculate_iv.R` → `{ iv: { feature: number }, woe: {...}, monotonicity: {...}, stability: {...} }`
* `calculate_psi.R` → `{ feature_psi: {...}, score_psi: number, sensitivity_analysis: {...} }`
* `train_model.R` → `{ model_coefficients: [...], diagnostics: {...}, rds_path: string }`
* `impute.R` → `{ imputed_csv: string, imputation_log: ImputationLogEntry[] }`

For codegen, the spec's `outputs` field lists the top-level keys the script promises to emit; the dry-run pass verifies the JSON parses and contains those keys.

### 9.8 Audit log entry

```typescript
interface AuditEntry {
  id: string;
  timestamp: string;        // ISO-8601
  agent: string;            // 'data_analyzer' | 'model_developer' |
                            // 'model_validator' | 'orchestrator' | 'system'
  action: string;           // stable verb (see §5.3 for the catalogue)
  details: string;          // free-form prose; LLM rationales land here
  actor: string | null;     // human reviewer name, or null
}
```

### 9.9 Filename conventions for downloads

Every download endpoint emits a stable, recognisable filename so a regulator can collect evidence by drag-and-drop without a per-file naming policy:

* `RiskCore_EDA_Report_<shortId>.md`
* `RiskCore_Model_Development_Report_<shortId>.md`
* `RiskCore_Validation_Report_<shortId>.md`
* `RiskCore_<shortId>_cleaned.csv` (and the four other handoff files)
* `RiskCore_<shortId>_model_<modelType>.rds`
* `RiskCore_<shortId>_model_<modelType>.RData`
* `RiskCore_LogJournal_<runId>.csv`

`<shortId>` is the first uuid block of the run id; `<runId>` is the full uuid.

---

## 10. Regulatory Mapping

This section maps system behaviour to specific regulatory expectations so a validator can reference the right artifact for each requirement.

| Requirement | Source | How RiskCore satisfies it | Where the evidence lives |
| --- | --- | --- | --- |
| Independent model validation (2nd line) | EBA GL 2017/16 §3; BaFin MaRisk AT 4.1 | The Model Validator agent runs after the Developer with no shared state besides typed outputs; it recomputes metrics independently and may issue a different verdict | `agent_results` row for `model_validator`; `validation_report.md`; `validationStatus` on the run |
| Full audit trail of decisions | Basel III Principle 7; EBA GL 2017/16 §4 | Append-only `audit_log` keyed by `(run_id, timestamp)`, capturing every LLM rationale and every human reviewer action; downloadable as the **Log Journal** CSV | `audit_log` table; `GET /api/pipeline/runs/:runId/journal/download` |
| LLM is a decision-maker, not a trainer | EBA GL 2017/16 §4.2; common supervisory expectation | All numeric outputs (IV, GLM coefficients, PSI, VIF) come from R; the LLM only chooses among R outputs and writes prose. The R model is downloadable as RDS/RData for independent re-execution | R script outputs; `model_*.rds`; `model_*.RData`; LLM rationale rows in `audit_log` |
| Reproducibility | Basel III Principle 7; SR 11-7 | Fixed seed (42) in `train_model.R`; the RData bundle packages dataframe + cleaned frame + splits + fitted GLM + metadata so a reviewer can `load()` and reproduce | `RiskCore_<shortId>_model_<modelType>.RData` |
| Data lineage | EBA GL 2017/16 §3.4 | `transformation_log.json` records every column dropped, every row dropped, every imputation, and every repair edit, with `triggered_by` for repair-step provenance | `transformation_log.json` in the handoff package |
| Population stability monitoring | EBA GL 2017/16 §6 | `calculate_psi.R` runs on every run; OOS holdout supported via `holdoutFileId`. PSI is summarised in the validator's prompt and stored in full in `psi_results` | `model_validator.output.psi_results`; validation report PSI section |
| Leakage / restricted attribute control | EBA GL 2017/16 §3.3 | Eligibility engine produces `EligibilityDecision` per feature; `leakage_watchlist` is surfaced to the validator | `feature_assessments[]`, `leakage_watchlist[]` in the analyzer output |
| Human review of generative-AI artifacts | Emerging supervisory practice (e.g. EU AI Act Article 14) | Codegen is opt-in per run, defaults to `codegen_review_required=true`, and unreviewed scripts are skipped. Every approval/rejection records reviewer + ISO timestamp + notes in the audit log | `generated_scripts` table; `codegen_script_approved/rejected` audit rows |
| Sandboxing of model code | OWASP-style defence-in-depth | LLM-generated R runs under wall-clock + `R_MAX_VSIZE` + AS-cap (`prlimit`) limits with a static-check pass that denies every primitive capable of escaping the sandbox | `r-sandbox.ts`; `codegen-checks.ts`; `static_check`/`dry_run` columns of `generated_scripts` |
| Right to model inspection | SR 11-7; PRA SS1/23 | Every artifact (datasets, RDS model, RData bundle, three reports, log journal CSV) is downloadable per run via stable URLs | §4.4–4.5; §9.9 |

---

## 11. Glossary

| Term | Definition |
| --- | --- |
| **PD** | Probability of Default — the probability a borrower will default within a given performance window. |
| **IV / WOE** | Information Value / Weight of Evidence. Standard univariate predictive-power measures used in retail credit scoring. IV ≥ 0.02 weak, 0.1 medium, 0.3 strong; > 0.5 is suspicious (likely target leakage). |
| **PSI** | Population Stability Index. PSI < 0.1 stable, 0.1–0.25 some shift, > 0.25 significant shift (action required). |
| **GLM (logistic)** | The R `glm(family=binomial)` fit. The champion model type by default. |
| **Challenger** | The non-champion model trained alongside; today gradient-boosting via the same `train_model.R`. |
| **Handoff package** | The versioned directory of files written by the Data Analyzer for the Model Developer to consume. v1 is the original; v2 is the result of one repair. |
| **Repair leg** | The orchestrator's one-shot retry path: the Modeller's typed feedback drives the Analyzer to drop / recode the offending column and emit a v2 handoff. |
| **Failure reason** | Stable enum on `pipeline_runs.failure_reason`. See §3.6. |
| **Script registry** | The set of R scripts (default + LLM-generated) the analyzer can pick from per step. |
| **Codegen** | LLM-generated R scripts produced when no registered script can satisfy a step. Always static-checked, dry-run, and (by default) held for human review. |
| **Sandbox** | The `r-sandbox.ts` runner: wall-clock + memory-capped per-call `Rscript` invocation with cwd pinned and outputs JSON-validated. Used **only** for LLM-generated scripts. |
| **R worker** | The long-lived `Rscript --vanilla worker-loop.R` process managed by `r-runner.ts`. Handles every trusted-pipeline `runRScript()` call for the lifetime of the api-server. Calls execute in a fresh child environment so top-level state cannot leak across runs. |
| **Audit log** | The append-only `audit_log` table. The single source of truth for "what happened, when, who decided what". |
| **Log Journal** | The downloadable CSV of the audit log enriched with R script names, LLM models/decisions, features, and metrics — the one-file evidence pack for a regulator. |
| **DATA_DIR** | The on-disk root for uploads and per-run artifacts (`data/` by default; configured in `r-runner.ts`). |

---

*End of document.*
*Generated against the `main` branch as of 2026-05-02. Revised 2026-05-02 to incorporate the persistent R worker (Task #35).*
