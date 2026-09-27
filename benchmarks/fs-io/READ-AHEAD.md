# After read-ahead in the FS reader

The same 66-run, 35-second protocol as [DURATION.md](DURATION.md), rerun on
the afternoon of September 25 after three changes to `normfs-fs` and none to
the harness: `ReadFile` fills a window ahead of the caller (64 KiB doubling to
1 MiB while drained, at most 2 MiB per fill) and keeps it across a seek inside
it; `Fs::mkdir_all` remembers directories it has already made durable; and
the executor shares one directory sync between publishes to the same parent.
Only the first reaches this harness: it never calls `mkdir_all`, and each
client publishes into its own directory.

Artifacts: [chart](results-2026-09-25-35s-readahead/raw-io-vs-fs.png),
[table](results-2026-09-25-35s-readahead/table.md),
[runs](results-2026-09-25-35s-readahead/runs.jsonl),
[metadata](results-2026-09-25-35s-readahead/metadata.json),
[duration audit](results-2026-09-25-35s-readahead/duration-audit.json).
All 66 runs passed the duration and byte audit; the shortest worker interval
was 35.000000041 s.

## Reads

| Case | FS vs Raw, morning | FS vs Raw, afternoon | FS CPU s/GiB, morning → afternoon |
|---|---:|---:|---:|
| 32 KiB, 1 client | −84.9% | −16.6% | 0.550 → 0.075 |
| 4 MiB, 1 client | −34.8% | −30.5% | 0.075 → 0.067 |
| 4 MiB, 8 clients | −31.3% | −29.1% | 0.133 → 0.131 |

The 32 KiB row is the one the window targets: the benchmark seeks to where the
reader already is before every read, and that seek now costs nothing, so a
1 MiB window serves 32 reads for one pool round trip. The remaining gap is the
copy from the window into the caller's buffer plus one hop per MiB.

The 4 MiB rows barely move because a 4 MiB request was already one or two
hops. A first attempt capped the fill at 4 MiB so a page was one hop; that
series (`results-2026-09-25-35s-readahead-4mib-fill/`, superseded, kept as
evidence) lost a quarter of the eight-client throughput to the 1 MiB cap, and
single 35-second probes put 2 MiB ahead of both. Raw CPU per GiB on the read
rows was unchanged between the series, so these rows are comparable.

## Writes

Every write ratio stayed inside the min–max spread of the morning series, in
both directions: 4 MiB single-client append went from −14.4% to −27.8%, 4 MiB
eight-client append from −1.1% to +14.3%. Per round, the backend that ran
first in a 4 MiB single-client pair was faster in five of six pairs across the
two series, whichever it was. Raw CPU per GiB on the write rows roughly
doubled between morning and afternoon (32 KiB append: 2.05 → 4.57), so the
host was not in the same state; the write path itself is unchanged by these
commits. These rows do not show a write regression, and they do not rule out a
few percent either way.
