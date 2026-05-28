# Lakebridge Enablement Demo

A simple, repeatable demo script for showing Lakebridge end-to-end during an enablement session. Two flows:

1. **BladeBridge** — Redshift → Databricks
2. **Morpheus** — Snowflake → Databricks

Sample SQL lives in `lakebridge_demo/redshift/` and `lakebridge_demo/snowflake/`.

## One-time setup

```
databricks auth login https://<workspace>.cloud.databricks.com --profile lakebridge-demo
```

```
CREATE CATALOG IF NOT EXISTS lakebridge_demo;
CREATE SCHEMA  IF NOT EXISTS lakebridge_demo.migration_test;
```

### Databricks-laptop gotcha

Maven Central and PyPI are blocked on Databricks laptops. Set proxies **before** installing:

```
sudo jamf policy
pip3 config set global.index-url https://pypi-proxy.cloud.databricks.com/simple
export LAKEBRIDGE_MAVEN_URL="https://maven-proxy.cloud.databricks.com"
echo 'export LAKEBRIDGE_MAVEN_URL="https://maven-proxy.cloud.databricks.com"' >> ~/.zshrc
```

If you launch the CLI from a GUI (IntelliJ, VS Code), also set it for launchd:

```
launchctl setenv LAKEBRIDGE_MAVEN_URL https://maven-proxy.cloud.databricks.com
```

Requires Lakebridge **v0.13.0+** — `LAKEBRIDGE_MAVEN_URL` is ignored on earlier versions.

### Install Lakebridge

```
databricks labs install lakebridge --profile lakebridge-demo
```

## Demo 1 — BladeBridge (Redshift → Databricks)

### Analyze

```
databricks labs lakebridge analyze \
  --source-directory $(pwd)/lakebridge_demo/redshift \
  --report-file      $(pwd)/lakebridge_demo/analyzer_output/redshift_inventory.xlsx \
  --source-tech      Redshift \
  --profile lakebridge-demo
```

Open `redshift_inventory.xlsx` and walk the **Summary** and **Unsupported** sheets.

### Install BladeBridge + transpile

```
databricks labs lakebridge install-transpile --profile lakebridge-demo
```

| Prompt | Answer |
|---|---|
| Source dialect | `redshift` |
| BladeBridge override config | leave empty (defaults) |
| Input SQL path | `$(pwd)/lakebridge_demo/redshift` (use absolute) |
| Output directory | `$(pwd)/lakebridge_demo/transpiled/redshift` |
| Error file path | `$(pwd)/lakebridge_demo/logs/redshift_errors.log` |
| Validate? | `yes` |
| Catalog / Schema | `lakebridge_demo` / `migration_test` |
| SQL Warehouse | any Serverless warehouse you can use |

```
databricks labs lakebridge transpile --profile lakebridge-demo
```

Open one converted file side-by-side with the source. Talk to:

- `DISTKEY`/`SORTKEY`/`DISTSTYLE` stripped — Delta auto-clusters.
- `IDENTITY(1,1)` → `GENERATED ALWAYS AS IDENTITY`.
- `SUPER` → `VARIANT`, `HLLSKETCH` → `FIXME` (no direct equivalent).
- Stored procedures with `REFCURSOR` / dynamic SQL → `FIXME` comments to rewrite.

## Demo 2 — Morpheus (Snowflake → Databricks)

### Analyze

```
databricks labs lakebridge analyze \
  --source-directory $(pwd)/lakebridge_demo/snowflake \
  --report-file      $(pwd)/lakebridge_demo/analyzer_output/snowflake_inventory.xlsx \
  --source-tech      Snowflake \
  --profile lakebridge-demo
```

### Re-install transpiler for Snowflake + transpile

```
databricks labs lakebridge install-transpile --profile lakebridge-demo
```

| Prompt | Answer |
|---|---|
| Override existing installation? | `yes` |
| Source dialect | `snowflake` |
| Input SQL path | `$(pwd)/lakebridge_demo/snowflake` |
| Output directory | `$(pwd)/lakebridge_demo/transpiled/snowflake` |
| Error file path | `$(pwd)/lakebridge_demo/logs/snowflake_errors.log` |
| Validate? | `yes` |
| Catalog / Schema | `lakebridge_demo` / `migration_test` |
| SQL Warehouse | any Serverless warehouse you can use |

```
databricks labs lakebridge transpile --profile lakebridge-demo
```

Walk one easy file (`03_medium_joins.sql` — clean conversion) and one hard file (`11_very_complex_procedure.sql` — Snowflake Scripting parse error is the known open gap). Talk to:

- `VARIANT`, `IFNULL`, `IFF`, `INITCAP`, `QUALIFY` map natively.
- `STREAM` / `TASK` → `FIXME`; rewrite as Lakeflow Declarative Pipelines / Jobs.
- `COPY INTO` translates; `SNOWPIPE` does not — use Auto Loader / Lakeflow Connect.
- Snowflake Scripting procs not yet parsed by Morpheus.

## Push converted code into the workspace

```
databricks workspace import-dir \
  $(pwd)/lakebridge_demo/transpiled/snowflake \
  /Workspace/Users/<you>/lakebridge_demo/snowflake \
  --overwrite --profile lakebridge-demo
```

Add to the top of each file:

```
USE CATALOG lakebridge_demo;
USE SCHEMA  migration_test;
```

Run a query, show it works.

## Troubleshooting cheats

```
# Did the transpiler JAR land?
ls -lh ~/.databricks/labs/remorph-transpilers/morpheus/lib/*.jar
ls -lh ~/.databricks/labs/remorph-transpilers/bladebridge/lib/*.jar

# LSP log if transpile fails
tail -f ~/.databricks/labs/remorph-transpilers/morpheus/lib/lsp-server.log

# Full reset
rm -rf ~/.databricks/labs/remorph ~/.databricks/labs/remorph-transpilers
databricks labs install lakebridge --profile lakebridge-demo
databricks labs lakebridge install-transpile --profile lakebridge-demo
```

## References

- Public docs: https://databrickslabs.github.io/lakebridge/
- GitHub: https://github.com/databrickslabs/lakebridge
- Maven proxy: `go/maven-registry-access`
- PyPI proxy: `go/pypi-registry-access`
- Slack: `#ststrack-data-warehousing`, `#lakebridge`
