# Render cargo-criterion `--message-format=json` output (newline-delimited)
# as a GitHub-flavored Markdown table for the job step summary.
#
# Usage: jq -nr -f .github/scripts/bench-summary.jq criterion-output.jsonl

def fmt_time(ns):
  if ns >= 1e9 then "\(((ns/1e9)*100|round)/100) s"
  elif ns >= 1e6 then "\(((ns/1e6)*100|round)/100) ms"
  elif ns >= 1e3 then "\(((ns/1e3)*100|round)/100) µs"
  else "\((ns*100|round)/100) ns" end;

def fmt_thr:
  if (.throughput | length) > 0 and .throughput[0].unit == "elements"
  then ((.throughput[0].per_iteration * 1e9 / .median.estimate) / 1e6) as $m
       | "\(($m*100|round)/100) Melem/s"
  else "—" end;

[ inputs | select(.reason == "benchmark-complete") ] as $rows
| if ($rows | length) == 0 then
    "## Benchmark results\n\n_No benchmark measurements were produced._"
  else
    "## Benchmark results\n",
    "\($rows | length) benchmarks · median time, mean time, and throughput.\n",
    "| Benchmark | Median | Mean | Throughput |",
    "| --- | ---: | ---: | ---: |",
    ($rows[] | "| `\(.id)` | \(fmt_time(.median.estimate)) | \(fmt_time(.mean.estimate)) | \(fmt_thr) |")
  end
