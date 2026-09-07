#!/usr/bin/env python3
"""dev vs stack, per host: propagation latency (time until every one of N polling
clients saw a new entry), medians across rounds, and where each version's
propagation departs from its small-N value (knee = first N where p99 > 2x the N=1 p99)."""
import re, sys, statistics
from collections import defaultdict

rows = defaultdict(list)
for path in sys.argv[1:]:
    for line in open(path):
        if 'RESULT' not in line:
            continue
        kv = dict(re.findall(r'(\w+)=([^\s]+)', line))
        rows[(kv['host'], kv['version'], int(kv['clients']))].append(kv)

def m(host, ver, n, f):
    rs = rows.get((host, ver, n))
    return statistics.median(float(r[f]) for r in rs) if rs else None

def fmt(us):
    return '   -   ' if us is None else (f'{us/1000:6.2f}ms' if us >= 1000 else f'{us:5.0f}us')

for host in sorted({k[0] for k in rows}):
    ns = sorted({k[2] for k in rows if k[0] == host})
    rounds = max(len(rows[k]) for k in rows if k[0] == host)
    print(f'\n== {host} · propagation to all N clients · medians of {rounds} rounds ==')
    print(f'{"N":>5} | {"dev p50":>8} {"dev p99":>8} {"dev cpu":>7} | {"stack p50":>9} {"stack p99":>9} {"stack cpu":>9} | {"p99 ratio":>9}')
    base = {v: m(host, v, ns[0], 'prop_p99_us') for v in ('dev', 'stack')}
    knee = {}
    for n in ns:
        d50, d99, dc = m(host,'dev',n,'prop_p50_us'), m(host,'dev',n,'prop_p99_us'), m(host,'dev',n,'cpu_s')
        s50, s99, sc = m(host,'stack',n,'prop_p50_us'), m(host,'stack',n,'prop_p99_us'), m(host,'stack',n,'cpu_s')
        ratio = f'{s99/d99:9.2f}' if (s99 and d99) else '     -   '
        print(f'{n:>5} | {fmt(d50)} {fmt(d99)} {"   -   " if dc is None else f"{dc:7.2f}"} | {fmt(s50)} {fmt(s99)} {"    -    " if sc is None else f"{sc:9.2f}"} | {ratio}')
        for v, p99 in (('dev', d99), ('stack', s99)):
            if v not in knee and p99 and base[v] and p99 > 2 * base[v]:
                knee[v] = n
    for v in ('dev', 'stack'):
        print(f'   {v}: propagation p99 doubles vs N=1 at N={knee[v]}' if v in knee else f'   {v}: no doubling in range')
