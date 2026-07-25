#!/bin/sh
# Decides whether a WP run actually proved anything.
#
# frama-c exits 0 whether or not it discharges every goal, so its exit status
# says only that the tool ran. Takes a -wp-report-json report or a console log;
# prefer the JSON, which states `passed` per goal.
set -eu

report="${1:?usage: check-proved.sh <wp-report.json|wp-log>}"

case "$report" in
*.json)
	if [ ! -s "$report" ]; then
		echo "check-proved: $report is empty; WP did not run" >&2
		exit 1
	fi

	total=$(jq 'length' "$report")
	if [ "$total" -eq 0 ]; then
		echo "check-proved: no goals in $report; WP proved nothing" >&2
		exit 1
	fi

	failed=$(jq '[.[] | select(.passed == false)] | length' "$report")
	if [ "$failed" -ne 0 ]; then
		echo "check-proved: $failed of $total goals not proved in $report" >&2
		jq -r '.[] | select(.passed == false)
		       | "  \(.goal) [\(if .smoke then "smoke" else "goal" end)] \(.verdict)"' \
			"$report" >&2
		exit 1
	fi

	# Without smoke tests a vacuous precondition reports green.
	smoke=$(jq '[.[] | select(.smoke == true)] | length' "$report")
	if [ "$smoke" -eq 0 ]; then
		echo "check-proved: no smoke tests in $report; run with -wp-smoke-tests" >&2
		exit 1
	fi
	;;
*)
	# Console-log fallback. Weaker: these patterns are presentation details.
	if ! grep -q "Proved goals:" "$report"; then
		echo "check-proved: no proof summary in $report; WP did not run" >&2
		exit 1
	fi

	if ! grep -q "Smoke Tests" "$report"; then
		echo "check-proved: no smoke-test summary in $report" >&2
		exit 1
	fi

	# Both counters must be exhausted.
	awk '
		/Proved goals:|Smoke Tests:/ {
			split($0, f, ":")
			split(f[2], g, "/")
			gsub(/[^0-9]/, "", g[1])
			gsub(/[^0-9]/, "", g[2])
			if (g[1] != g[2]) {
				printf "check-proved: %s of %s in %s\n", g[1], g[2], $0 > "/dev/stderr"
				bad = 1
			}
		}
		END { exit bad }
	' "$report"
	;;
esac
