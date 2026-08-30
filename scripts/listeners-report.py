#!/usr/bin/env python3
"""Aggregate RESULT lines from listeners-sweep logs into medians per (host, mode, N)
and point at the knee: the first N where p99 lag, enqueue p99, or CPU bends up."""
import re, sys, statistics
from collections import defaultdict

rows = defaultdict(list)  # (host, mode, n) -> list of dicts
for path in sys.argv[1:]:
    for line in open(path):
        if 'RESULT' not in line:
            continue
        kv = dict(re.findall(r'(\w+)=([^\s]+)', line))
        key = (kv['host'], kv['mode'], int(kv['listeners']))
        rows[key].append(kv)

def med(vals):
    return statistics.median(vals)

def fmt_us(us):
    return f'{us/1000:.2f} ms' if us >= 1000 else f'{us:.0f} us'

hosts = sorted({k[0] for k in rows})
for host in hosts:
    for mode in ('follow', 'poll'):
        keys = sorted(k for k in rows if k[0] == host and k[1] == mode)
        if not keys:
            continue
        print(f'\n== {host} · {mode} · medians of {max(len(rows[k]) for k in keys)} rounds ==')
        print(f'{"N":>5} {"enq p50":>9} {"enq p99":>9} {"lag p50":>10} {"lag p99":>10} {"lag max":>10} {"deliv%":>7} {"drops":>5} {"cpu s":>6} {"rss":>6}')
        prev = None
        knee = None
        for k in keys:
            rs = rows[k]
            g = lambda f: med([float(r[f]) for r in rs])
            deliv = 100.0 * g('delivered') / max(1.0, g('expected'))
            row = dict(n=k[2], ep50=g('enqueue_p50_us'), ep99=g('enqueue_p99_us'),
                       lp50=g('lag_p50_us'), lp99=g('lag_p99_us'), lmax=g('lag_max_us'),
                       deliv=deliv, drops=g('dropped_subs'), cpu=g('cpu_s'), rss=g('rss_mib'))
            print(f'{row["n"]:>5} {fmt_us(row["ep50"]):>9} {fmt_us(row["ep99"]):>9} {fmt_us(row["lp50"]):>10} '
                  f'{fmt_us(row["lp99"]):>10} {fmt_us(row["lmax"]):>10} {row["deliv"]:>6.1f}% {row["drops"]:>5.0f} '
                  f'{row["cpu"]:>6.2f} {row["rss"]:>5.0f}M')
            # knee: p99 lag more than doubles vs previous step while N doubles,
            # or delivery drops below 100%, or subscriptions drop
            if prev is not None and knee is None:
                if row['lp99'] > 2.0 * prev['lp99'] and row['lp99'] > 2000:
                    knee = (row['n'], 'p99 lag doubled')
                elif row['deliv'] < 99.9:
                    knee = (row['n'], 'records missed')
                elif row['drops'] > 0:
                    knee = (row['n'], 'subscriptions dropped')
                elif row['cpu'] > g('wall_s'):
                    knee = (row['n'], 'more than one core busy')
            prev = row
        print(f'   knee: {knee[0]} listeners ({knee[1]})' if knee else '   knee: not reached in this range')
