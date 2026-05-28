# Lakebridge End-to-End Demo — Snowflake & Redshift

A reproducible, customer-ready walkthrough of **Databricks Lakebridge**:

1. **Analyzer** — scans source warehouse code, scores complexity, produces an Excel inventory.
2. **Transpiler** — converts Snowflake / Redshift / Teradata / Oracle / Synapse SQL into Databricks SQL.
3. **(Optional)** **Reconcile** — validates row counts / values between source and target after migration.

This demo focuses on the **Snowflake → Databricks** path end-to-end (using the **Morpheus** transpiler) and includes a parallel **Redshift** corpus for comparison. It is hardened for the most common failure mode field engineers see on Databricks-issued laptops: **outbound access to Maven Central is blocked**, so `install-transpile` fails to pull the Morpheus JARs unless you point the installer at the internal Maven proxy via `LAKEBRIDGE_MAVEN_URL`.

---

## Folder layout

```
lakebridge_demo/
├── README.md                      ← this file
├── snowflake/                     ← input: 12 graduated Snowflake SQL samples
│   ├── 01_simple_ddl.sql
│   ├── 02_simple_select.sql
│   ├── 03_medium_joins.sql
│   ├── 04_medium_semi_structured.sql
│   ├── 05_medium_pivot_qualify.sql
│   ├── 06_medium_time_travel.sql
│   ├── 07_complex_streams_tasks.sql
│   ├── 08_complex_udfs.sql
│   ├── 09_complex_copy_pipe.sql
│   ├── 10_complex_dynamic_tables.sql
│   ├── 11_very_complex_procedure.sql
│   └── 12_very_complex_etl.sql
├── redshift/                      ← input: 4 graduated Redshift SQL samples
│   ├── 01_simple.sql
│   ├── 02_medium.sql
│   ├── 03_complex.sql
│   └── 04_very_complex.sql
├── transpiled/
│   ├── snowflake/                 ← output: Databricks SQL after transpile
│   └── redshift/
├── analyzer_output/               ← Excel inventory + JSON report
└── logs/                          ← snowflake_errors.log, redshift_errors.log, lsp-server.log
```

Why the graduated corpus? It surfaces transpiler behaviour you only see at scale: stored procedures, `STREAMS`/`TASKS`, `DYNAMIC TABLE`, semi-structured `VARIANT`/`SUPER`, MERGE, HLL sketches, and PL/pgSQL — exactly the features that decide whether a real migration is two weeks or two quarters.

---

## Prerequisites

| Component | Required version | Verify |
|---|---|---|
| Python | **3.10.x – 3.13.x** (Python 3.14 is NOT supported as of May 2026) | `python3 --version` |
| Java   | **JDK 11+** (required by Morpheus / ANTLR runtime) | `java -version` |
| Databricks CLI | **v0.240+** (anything newer than the `databricks labs` redesign) | `databricks --version` |
| `databricks auth login` | A working profile pointing at a workspace with a SQL Warehouse | `databricks auth profiles` |

### macOS one-liner (Databricks laptops)

```bash
brew install python@3.12 openjdk@21
brew install databricks/tap/databricks
sudo ln -sfn $(brew --prefix openjdk@21)/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk-21.jdk
```

### Workspace authentication

Use a named profile so you can pass `--profile` everywhere — multi-workspace SAs *will* footgun themselves with the default profile otherwise.

```bash
databricks auth login \
  https://e2-demo-field-eng.cloud.databricks.com \
  --profile lakebridge-demo
```

Confirm:

```bash
databricks current-user me --profile lakebridge-demo
```

---

## Step 1 — Install Lakebridge (base CLI)

```bash
databricks labs install lakebridge --profile lakebridge-demo
```

Expected (truncated):

```
INFO [d.l.lakebridge.base_install] Successfully Setup Lakebridge Components Locally
INFO [d.l.lakebridge.base_install] For more information, please visit https://databrickslabs.github.io/lakebridge/
```

Smoke-test:

```bash
databricks labs lakebridge --help
```

You should see at least these subcommands: `analyze`, `install-transpile`, `transpile`, `configure-reconcile`, `reconcile`.

---

## Step 2 — The Databricks-laptop gotcha (READ THIS FIRST)

> **TL;DR — Before you run `install-transpile`, export `LAKEBRIDGE_MAVEN_URL`. If you skip this on a Databricks laptop the installer hangs on the Morpheus download or fails with an SSL / connection-refused error pulling from `repo1.maven.org`.**

### Why this happens

The `install-transpile` step downloads the **Morpheus** transpiler JAR (Snowflake / Teradata / Oracle / Synapse parser) from Maven Central. Databricks-issued laptops have outbound HTTPS to public Maven hosts (`repo1.maven.org`, `central.sonatype.com`) blocked by the corporate proxy / Zscaler config. The installer does not retry against an internal mirror automatically — you have to tell it.

### The fix

Point the installer at the internal Maven proxy **before** running `install-transpile`:

```bash
export LAKEBRIDGE_MAVEN_URL="https://maven-proxy.cloud.databricks.com"
```

Persist it for future shells:

```bash
echo 'export LAKEBRIDGE_MAVEN_URL="https://maven-proxy.cloud.databricks.com"' >> ~/.zshrc
# or ~/.bashrc on bash
```

If the proxy needs credentials, put them in `~/.netrc` (Lakebridge picks this up automatically — you can override the file with the `NETRC` env var):

```
machine maven-proxy.cloud.databricks.com
  login   <your-databricks-email>
  password <your-token-or-pat>
```

```bash
chmod 600 ~/.netrc
```

### Other failure modes on Databricks laptops

| Symptom | Root cause | Fix |
|---|---|---|
| `SSLError: CERTIFICATE_VERIFY_FAILED` during `install-transpile` | Corporate cert chain not in Python's trust store | `pip install --upgrade certifi` then `export REQUESTS_CA_BUNDLE=$(python3 -m certifi)` |
| `install-transpile` hangs at "Downloading transpiler..." | Maven Central blocked, `LAKEBRIDGE_MAVEN_URL` not set | Export the variable above |
| `java: command not found` mid-install | JDK installed via Homebrew cask but not on `PATH` | `export PATH="$(brew --prefix openjdk@21)/bin:$PATH"` |
| `databricks labs install lakebridge` says "permission denied" writing under `~/.databricks/labs/` | Stale ownership from a previous root install | `sudo chown -R $(whoami):staff ~/.databricks` |
| Validation step errors with `INSUFFICIENT_PERMISSIONS` | Service principal lacks Unity Catalog grants on the catalog/schema | Grant `USE CATALOG, USE SCHEMA, CREATE TABLE, MODIFY` or just `ALL PRIVILEGES` on the demo catalog |

---

## Step 3 — Install the Morpheus transpiler

> Morpheus is the **deterministic** Snowflake / Teradata / Oracle / Synapse SQL transpiler. It is the one you want for SQL-heavy migrations because its output is reproducible (no LLM in the hot path).

Make sure the env var from Step 2 is set in your shell:

```bash
echo $LAKEBRIDGE_MAVEN_URL
# → https://maven-proxy.cloud.databricks.com
```

Now run install-transpile and answer the prompts. Use **absolute paths** — relative paths bite later when the transpiler runs from a different cwd.

```bash
databricks labs lakebridge install-transpile --profile lakebridge-demo
```

Recommended answers for the Snowflake (Morpheus) flow:

| Prompt | Answer |
|---|---|
| Do you want to override the existing installation? | `yes` (first run: choose `no` on the override prompt if you haven't installed yet — there's nothing to override) |
| Source dialect | `snowflake` (or the number next to it in the list — currently `7`, verify when you see the list) |
| Input SQL path | `/Users/<you>/Documents/customer_demos/lakebridge_demo/snowflake` |
| Output directory | `/Users/<you>/Documents/customer_demos/lakebridge_demo/transpiled/snowflake` |
| Error file path | `/Users/<you>/Documents/customer_demos/lakebridge_demo/logs/snowflake_errors.log` |
| Validate the syntax and semantics of the transpiled queries? | `yes` *(only if you have a SQL Warehouse + UC grants — see below)* |
| Catalog name | `lakebridge_demo` |
| Schema name | `migration_test` |
| SQL Warehouse | pick a Serverless warehouse you have `CAN_USE` on (e.g. `dbdemos-shared-endpoint` on `e2-demo-field-eng`) |
| Open the config file? | `yes` (a browser tab opens with the generated `config.yml`) |

> **Pre-create the validation catalog & schema** in a SQL editor — Morpheus assumes both exist:
>
> ```sql
> CREATE CATALOG IF NOT EXISTS lakebridge_demo;
> CREATE SCHEMA  IF NOT EXISTS lakebridge_demo.migration_test;
> GRANT ALL PRIVILEGES ON CATALOG lakebridge_demo TO `<your-email>`;
> ```

The generated config lives at `~/.databricks/labs/remorph/config.yml` (the project hasn't renamed every internal path from `remorph` → `lakebridge` yet — don't be surprised).

---

## Step 4 — Run the Analyzer (inventory before you transpile)

> Always **analyze before you transpile**. The Excel report tells the customer how much of their estate is "easy / medium / hard" and surfaces unsupported constructs *before* you spend time on the conversion.

```bash
databricks labs lakebridge analyze \
  --source-directory /Users/<you>/Documents/customer_demos/lakebridge_demo/snowflake \
  --report-file     /Users/<you>/Documents/customer_demos/lakebridge_demo/analyzer_output/snowflake_inventory.xlsx \
  --source-tech     Snowflake \
  --generate-json   true \
  --profile lakebridge-demo
```

> The `--source-tech` flag is case-sensitive *for the display name* but the CLI is forgiving — if you omit it, an interactive menu lists every supported source (Snowflake is currently option `25` in the menu, but read the screen rather than hard-coding the number).

What you get:

- `snowflake_inventory.xlsx` — multi-sheet workbook with:
  - **Summary** — file counts by complexity bucket
  - **Files** — every file with LOC, statement counts, complexity score
  - **Constructs** — Snowflake-specific syntax found per file (`VARIANT`, `STREAM`, `TASK`, `DYNAMIC TABLE`, `MERGE`, scripting blocks, etc.)
  - **Unsupported** — anything the transpiler will skip or FIXME
- `snowflake_inventory.json` — same data, for piping into a dashboard or another tool

Repeat for Redshift:

```bash
databricks labs lakebridge analyze \
  --source-directory /Users/<you>/Documents/customer_demos/lakebridge_demo/redshift \
  --report-file     /Users/<you>/Documents/customer_demos/lakebridge_demo/analyzer_output/redshift_inventory.xlsx \
  --source-tech     Redshift \
  --generate-json   true \
  --profile lakebridge-demo
```

---

## Step 5 — Run the Transpiler (Snowflake → Databricks via Morpheus)

If you accepted the defaults during `install-transpile`, this is a single command:

```bash
databricks labs lakebridge transpile --profile lakebridge-demo
```

To override any value at runtime (useful when you want to re-run against the Redshift corpus without re-installing):

```bash
databricks labs lakebridge transpile \
  --source-dialect    snowflake \
  --input-source      /Users/<you>/Documents/customer_demos/lakebridge_demo/snowflake \
  --output-folder     /Users/<you>/Documents/customer_demos/lakebridge_demo/transpiled/snowflake \
  --error-file-path   /Users/<you>/Documents/customer_demos/lakebridge_demo/logs/snowflake_errors.log \
  --catalog-name      lakebridge_demo \
  --schema-name       migration_test \
  --skip-validation   false \
  --profile lakebridge-demo
```

Add `--debug` if the run fails — it dumps the parse tree and the LSP traffic to `lsp-server.log`.

### Expected results against this corpus

| File | Bucket | Morpheus behaviour |
|---|---|---|
| `01_simple_ddl.sql` | simple | ✅ clean — `NUMBER`, `VARIANT`, `CLUSTER BY` get rewritten to Delta-compatible types and `CLUSTER BY` is preserved |
| `02_simple_select.sql` | simple | ✅ clean — `IFNULL`, `IFF`, `INITCAP`, `ILIKE` mapped to ANSI equivalents |
| `03_medium_joins.sql` | medium | ✅ — CTEs, window functions, `QUALIFY` translated; `NVL2`, `DIV0`, `ZEROIFNULL` mapped |
| `04_medium_semi_structured.sql` | medium | ⚠️ warnings — `PARSE_JSON`, `FLATTEN`, `OBJECT_CONSTRUCT` translated, but check the FIXME comments around `LATERAL FLATTEN` if you use deep paths |
| `05_medium_pivot_qualify.sql` | medium | ✅ clean — Databricks SQL supports `QUALIFY` and `PIVOT` natively |
| `06_medium_time_travel.sql` | medium | ⚠️ warnings — Snowflake `AT (OFFSET => …)` ≠ Delta time travel; expect a FIXME |
| `07_complex_streams_tasks.sql` | complex | ❌ **STREAM / TASK have no Databricks equivalent**; Morpheus emits the DDL as commented-out FIXMEs. Use Delta Live Tables / Lakeflow Jobs as the rewrite. |
| `08_complex_udfs.sql` | complex | ⚠️ — SQL UDFs convert cleanly; JavaScript / Python UDFs become FIXMEs |
| `09_complex_copy_pipe.sql` | complex | ❌ — `COPY INTO` translates but `SNOWPIPE` does not; replace with Auto Loader / Lakeflow Connect |
| `10_complex_dynamic_tables.sql` | complex | ⚠️ — `DYNAMIC TABLE` becomes a materialized-view stub; recommend DLT |
| `11_very_complex_procedure.sql` | very_complex | ❌ **Snowflake Scripting stored procedures are NOT yet parseable** by Morpheus (as of May 2026). Expect a parse error in `snowflake_errors.log`. This is the headline "open gap" to flag in customer conversations. |
| `12_very_complex_etl.sql` | very_complex | ⚠️ — large MERGE + scripting; partial success — review FIXMEs |

Open `logs/snowflake_errors.log` and `transpiled/snowflake/*.sql` after the run — every transpiled file annotates unsupported constructs inline with `-- FIXME` comments, which is the customer-facing artifact you want to grep.

---

## Step 6 — (Optional) Run the Redshift corpus

Redshift uses the **BladeBridge** transpiler, not Morpheus. The flow is the same, just re-run `install-transpile` and pick `redshift`:

```bash
databricks labs lakebridge install-transpile --profile lakebridge-demo
# Override existing installation? yes
# Source dialect? redshift
# (BladeBridge will prompt for a sample override config — leave blank to use defaults)
```

Then either accept the saved config:

```bash
databricks labs lakebridge transpile --profile lakebridge-demo
```

Or override at the CLI:

```bash
databricks labs lakebridge transpile \
  --source-dialect    redshift \
  --input-source      /Users/<you>/Documents/customer_demos/lakebridge_demo/redshift \
  --output-folder     /Users/<you>/Documents/customer_demos/lakebridge_demo/transpiled/redshift \
  --error-file-path   /Users/<you>/Documents/customer_demos/lakebridge_demo/logs/redshift_errors.log \
  --catalog-name      lakebridge_demo \
  --schema-name       migration_test \
  --skip-validation   false \
  --profile lakebridge-demo
```

> **BladeBridge gotcha:** if you need to customise variable prefixes, datatype overrides, or rewrite rules, point `--transpiler-config-path` at a JSON override file. A sample template lives in the upstream `lakebridge_lab_resources/sample_override.json` (clone `https://github.com/databrickslabs/lakebridge-lab-resources` if you don't already have it).

---

## Step 7 — Push the converted code into Databricks

The transpiler writes flat `.sql` files locally. To run them in Databricks:

```bash
databricks workspace import-dir \
  /Users/<you>/Documents/customer_demos/lakebridge_demo/transpiled/snowflake \
  /Workspace/Users/<your-email>/lakebridge_demo/snowflake \
  --overwrite \
  --profile lakebridge-demo
```

Inside each file, prepend:

```sql
USE CATALOG lakebridge_demo;
USE SCHEMA  migration_test;
```

Then open the file as a SQL editor query / notebook cell and execute against the same warehouse you used for validation.

---

## Step 8 — (Optional) Reconcile

Once tables exist on both sides, prove parity:

```bash
databricks labs lakebridge configure-reconcile --profile lakebridge-demo
# answer prompts for source (Snowflake), target (Databricks), connection details
databricks labs lakebridge reconcile --profile lakebridge-demo
```

Reconcile produces a row-count + sampled-value diff report and writes it to a Delta table in your demo catalog — perfect leave-behind for the customer.

---

## Demo script (15 minutes, customer-facing)

| Minute | Beat | What you show |
|---|---|---|
| 0–1 | Pitch | "Lakebridge automates the four steps every migration repeats: assess, convert, validate, reconcile." |
| 1–3 | `lakebridge_demo/snowflake/` | Skim 2 files — point out `VARIANT`, `STREAM`, `DYNAMIC TABLE`, scripting. "This is what a real estate looks like." |
| 3–5 | Run analyzer | Open `analyzer_output/snowflake_inventory.xlsx`. Walk the **Summary** + **Unsupported** sheets. |
| 5–10 | Run transpile | Watch the CLI; open one transpiled file (e.g. `03_medium_joins.sql`) side-by-side with the source. Point out preserved semantics. |
| 10–12 | Show a failure | Open `11_very_complex_procedure.sql` and `snowflake_errors.log`. Be honest: "Scripting is the open gap; here's the workaround." |
| 12–14 | Push to workspace | `import-dir` + run one query against UC. |
| 14–15 | Reconcile teaser | "After cutover we'd run reconcile against your real tables — here's what the report looks like." |

---

## Troubleshooting cheatsheet

```bash
# Verify the Morpheus install actually pulled the JAR
ls -lh ~/.databricks/labs/remorph-transpilers/morpheus/lib/*.jar

# Tail the LSP server log during a transpile run
tail -f ~/.databricks/labs/remorph-transpilers/bladebridge/lib/lsp-server.log    # BladeBridge
tail -f ~/.databricks/labs/remorph-transpilers/morpheus/lib/lsp-server.log      # Morpheus

# Re-bootstrap from scratch when in doubt
rm -rf ~/.databricks/labs/remorph ~/.databricks/labs/remorph-transpilers
databricks labs install lakebridge --profile lakebridge-demo
databricks labs lakebridge install-transpile --profile lakebridge-demo
```

---

## Reference links

- Lakebridge docs: https://databrickslabs.github.io/lakebridge/
- Installation page: https://databrickslabs.github.io/lakebridge/docs/installation/
- Analyzer page: https://databrickslabs.github.io/lakebridge/docs/assessment/analyzer/
- Transpile page: https://databrickslabs.github.io/lakebridge/docs/transpile/
- GitHub repo (latest releases, issues): https://github.com/databrickslabs/lakebridge
- Lab resources (sample inputs + overrides): https://github.com/databrickslabs/lakebridge-lab-resources
- Customer-facing solution page: https://www.databricks.com/solutions/migration/lakebridge

---

*Last verified: May 2026 against Lakebridge CLI ≥ 0.10. If the dialect numbers shift in a new release, trust the interactive prompt over the table above.*
