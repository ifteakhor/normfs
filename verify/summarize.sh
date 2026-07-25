#!/bin/sh
# Renders a WP run as a markdown table for $GITHUB_STEP_SUMMARY, so the goal
# counts land on the run page instead of only in the log artifact.
set -eu

logdir="${1:?usage: summarize.sh <log-dir> [machdep]}"
machdep="${2:-}"

field() {
	# "  Qed:  276" -> "276", empty when the line is absent
	sed -n "s/^ *$2: *\([0-9]*\).*/\1/p" "$1" | head -1
}

ratio() {
	sed -n "s/.*$2: *\([0-9]*\) *\/ *\([0-9]*\).*/\1 \2/p" "$1" | head -1
}

if [ -n "$machdep" ]; then
	echo "## Proofs — \`$machdep\`"
else
	echo "## Proofs"
fi
echo

printf '| target | goals | smoke | Qed | Alt-Ergo | Z3 |\n'
printf '|---|---|---|---|---|---|\n'

any_short=0
seen=0
for log in "$logdir"/verify-*.log; do
	[ -e "$log" ] || continue
	target=$(basename "$log" .log)
	seen=$((seen + 1))

	set -- $(ratio "$log" "Proved goals")
	proved=${1:-0}
	total=${2:-0}
	set -- $(ratio "$log" "Smoke Tests")
	sm_ok=${1:-0}
	sm_all=${2:-0}

	mark="✅"
	if [ "$proved" != "$total" ] || [ "$sm_ok" != "$sm_all" ]; then
		mark="❌"
		any_short=1
	fi

	printf '| `%s` | %s %s / %s | %s / %s | %s | %s | %s |\n' \
		"$target" "$mark" "$proved" "$total" "$sm_ok" "$sm_all" \
		"$(field "$log" Qed)" "$(field "$log" Alt-Ergo)" "$(field "$log" Z3)"
done

echo

if [ "$seen" -eq 0 ]; then
	echo '> No proof logs found — WP did not run.'
elif [ "$any_short" -eq 1 ]; then
	echo '<details><summary>Goals left unproved</summary>'
	echo
	echo '```'
	for log in "$logdir"/verify-*.log; do
		[ -e "$log" ] || continue
		grep -E "^\[wp\] \[(Unsuccess|Failed|Timeout)\]" "$log" |
			sed 's/^\[wp\] //; s/ (Cache miss.*//' || true
	done
	echo '```'
	echo '</details>'
	echo
	echo '> A goal left open is not a proof. See `verify/check-proved.sh`.'
else
	echo '> Every goal discharged, smoke tests included.'
fi
