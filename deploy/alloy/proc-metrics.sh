#!/usr/bin/env sh
# Per-process CPU and memory for macOS, written as a node_exporter textfile.
#
# macOS has no /proc, so Alloy's process exporter cannot run there. This takes
# one `ps` sample, sums processes into groups by executable name, and writes
# the same three metric families, with the same `groupname` label, that
# config.linux.alloy keeps from the process exporter:
#
#   namedprocess_namegroup_cpu_seconds_total{groupname}
#   namedprocess_namegroup_memory_bytes{groupname,memtype="resident"}
#   namedprocess_namegroup_num_procs{groupname}
#
# Cardinality is capped per run: the top 15 groups by CPU and the top 15 by
# resident memory keep their own series; everything else is summed into
# groupname="other". At most 31 groups x 3 series = 93 series per host.
#
# `ps` only reports a rate (%cpu), so the cpu counter is that rate integrated
# over the time since the previous run. It is an estimate, monotonic per
# groupname while the group stays in the top set; a group that drops into
# "other" and returns restarts from zero, which rate() reads as a reset.
#
# Run it once per interval from launchd (see proc-metrics.plist.example); it is
# not a daemon. Usage: proc-metrics.sh [OUTPUT_DIR]   (default FLEET_TEXTFILE_DIR)
#
# Test hooks: PROC_METRICS_PS replaces the ps command (it is run as
# `$PROC_METRICS_PS -axo pcpu=,rss=,comm=`), PROC_METRICS_NOW replaces the
# clock (epoch seconds).
set -eu
LC_ALL=C
export LC_ALL

out_dir="${1:-${FLEET_TEXTFILE_DIR:-}}"
[ -n "$out_dir" ] || { echo "usage: proc-metrics.sh OUTPUT_DIR (or set FLEET_TEXTFILE_DIR)" >&2; exit 2; }
[ -d "$out_dir" ] || { echo "proc-metrics.sh: $out_dir is not a directory" >&2; exit 2; }

ps_cmd="${PROC_METRICS_PS:-ps}"
now="${PROC_METRICS_NOW:-$(date +%s)}"
# Only *.prom files are read by the textfile collector, so the state file and
# the temp files below are invisible to it.
prom="$out_dir/proc-metrics.prom"
state="$out_dir/.proc-metrics.state"

# A run that does not finish removes the previous textfile: the collector
# would keep serving its old gauges as if they were current, and a gap in the
# series is the honest answer to a failed sample.
tmp_prom=""
tmp_state=""
published=0
cleanup() {
	rm -f "$tmp_prom" "$tmp_state"
	[ "$published" -eq 1 ] || rm -f "$prom"
}
trap cleanup EXIT
trap 'exit 1' HUP INT TERM
tmp_prom="$(mktemp "$out_dir/.proc-metrics.prom.XXXXXX")"
tmp_state="$(mktemp "$out_dir/.proc-metrics.state.XXXXXX")"
# Final mode before any byte lands; mktemp creates 0600 and Alloy may run as
# a different user than the launchd job.
chmod 0644 "$tmp_prom"
chmod 0600 "$tmp_state"

# Sample first so a failing ps aborts the run instead of publishing an empty file.
# shellcheck disable=SC2086 # PROC_METRICS_PS may carry arguments
sample="$($ps_cmd -axo pcpu=,rss=,comm=)"
printf '%s\n' "$sample" | awk -v now="$now" -v state="$state" -v prom="$tmp_prom" -v newstate="$tmp_state" '
function pick(metric, limit, needpositive,    k, name, best, bestv, taken) {
	for (k = 0; k < limit; k++) {
		best = ""
		for (name in cpu) {
			if (taken[name]) continue
			v = (metric == "cpu") ? cpu[name] : rss[name]
			if (needpositive && v <= 0) continue
			if (best == "" || v > bestv || (v == bestv && name < best)) { best = name; bestv = v }
		}
		if (best == "") return
		taken[best] = 1
		keep[best] = 1
	}
}
BEGIN {
	prev_ts = 0
	while ((getline line < state) > 0) {
		n = split(line, f, "\t")
		if (f[1] == "ts") prev_ts = f[2] + 0
		else if (f[1] == "c") total[f[2]] = f[3] + 0
	}
	close(state)
	dt = now - prev_ts
	if (prev_ts == 0 || dt < 0 || dt > 120) dt = 0
}
{
	if (NF < 3) next
	pct = $1 + 0
	kib = $2 + 0
	name = $0
	sub(/^ *[^ ]+ +[^ ]+ +/, "", name)
	sub(/.*\//, "", name)
	gsub(/[^A-Za-z0-9._ ()+@:-]/, "_", name)
	sub(/ +$/, "", name)
	name = substr(name, 1, 32)
	if (name == "") name = "unknown"
	if (name == "other") name = "other_"
	cpu[name] += pct
	rss[name] += kib
	procs[name]++
}
END {
	pick("cpu", 15, 1)
	pick("rss", 15, 0)
	for (name in cpu) {
		g = keep[name] ? name : "other"
		gcpu[g] += cpu[name]
		grss[g] += rss[name]
		gprocs[g] += procs[name]
	}
	count = 0
	for (g in gcpu) order[++count] = g
	for (i = 2; i <= count; i++) {
		x = order[i]
		for (j = i - 1; j >= 1 && order[j] > x; j--) order[j + 1] = order[j]
		order[j + 1] = x
	}
	printf "ts\t%d\n", now > newstate
	for (i = 1; i <= count; i++) {
		g = order[i]
		seconds[g] = total[g] + gcpu[g] / 100 * dt
		printf "c\t%s\t%.3f\n", g, seconds[g] > newstate
	}
	print "# HELP namedprocess_namegroup_cpu_seconds_total Estimated CPU time of the process group, integrated from ps %cpu." > prom
	print "# TYPE namedprocess_namegroup_cpu_seconds_total counter" > prom
	for (i = 1; i <= count; i++) printf "namedprocess_namegroup_cpu_seconds_total{groupname=\"%s\"} %.3f\n", order[i], seconds[order[i]] > prom
	print "# HELP namedprocess_namegroup_memory_bytes Resident memory of the process group." > prom
	print "# TYPE namedprocess_namegroup_memory_bytes gauge" > prom
	for (i = 1; i <= count; i++) printf "namedprocess_namegroup_memory_bytes{groupname=\"%s\",memtype=\"resident\"} %.0f\n", order[i], grss[order[i]] * 1024 > prom
	print "# HELP namedprocess_namegroup_num_procs Number of processes in the group." > prom
	print "# TYPE namedprocess_namegroup_num_procs gauge" > prom
	for (i = 1; i <= count; i++) printf "namedprocess_namegroup_num_procs{groupname=\"%s\"} %d\n", order[i], gprocs[order[i]] > prom
}'

# State first: if the second rename fails, the next run still continues from the
# newer totals, so the published counter never goes backwards.
mv "$tmp_state" "$state"
mv "$tmp_prom" "$prom"
published=1
