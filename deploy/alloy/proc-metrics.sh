#!/usr/bin/env sh
# Per-process CPU and memory for Linux and macOS, written as a node_exporter
# textfile.
#
# One `ps` sample is summed into groups by executable name and written as
# three metric families with a `groupname` label (the names the Alloy process
# exporter uses, so one query covers every host):
#
#   namedprocess_namegroup_cpu_seconds_total{groupname}
#   namedprocess_namegroup_memory_bytes{groupname,memtype="resident"}
#   namedprocess_namegroup_num_procs{groupname}
#
# Why a script and not Alloy's process exporter: the exporter cannot cap the
# number of groups, and the free Grafana Cloud tier allows 10k active series
# for the whole account. Here only the top 15 groups by CPU over the last
# interval and the top 15 by resident memory keep their own series; everything
# else is summed into groupname="other". At most 31 groups x 3 series = 93
# series per host, however many distinct programs run.
#
# CPU: `ps` reports each process's cumulative CPU time. The previous run's
# per-process totals are kept in a state file; the growth since then is the CPU
# used in the interval, and the per-group sum of those deltas accumulates into
# the counter. A process first seen since the last run contributes all of its
# CPU time. CPU a process used between its last sample and its exit is not seen,
# so very short-lived processes are undercounted. A gap over two minutes (a
# sleeping laptop), or no state, only re-baselines: nothing is added.
# A group that drops out of the top set and returns restarts from zero, which
# rate() reads as a counter reset.
#
# Linux kernel threads (pid 2 and its children) are skipped.
#
# Run it once per interval from launchd or a systemd timer (see
# proc-metrics.plist.example, proc-metrics.service.example and
# proc-metrics.timer.example); it is not a daemon.
# Usage: proc-metrics.sh [OUTPUT_DIR]   (default FLEET_TEXTFILE_DIR)
#
# Test hooks: PROC_METRICS_PS replaces the ps command (run as
# `$PROC_METRICS_PS <flags> -o pid=,ppid=,time=,rss=,comm=`), PROC_METRICS_NOW
# replaces the clock (epoch seconds), PROC_METRICS_OS replaces `uname -s`.
set -eu
LC_ALL=C
export LC_ALL

out_dir="${1:-${FLEET_TEXTFILE_DIR:-}}"
[ -n "$out_dir" ] || { echo "usage: proc-metrics.sh OUTPUT_DIR (or set FLEET_TEXTFILE_DIR)" >&2; exit 2; }
[ -d "$out_dir" ] || { echo "proc-metrics.sh: $out_dir is not a directory" >&2; exit 2; }

ps_cmd="${PROC_METRICS_PS:-ps}"
now="${PROC_METRICS_NOW:-$(date +%s)}"
os="${PROC_METRICS_OS:-$(uname -s)}"
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
	# Empty until mktemp succeeds; never hand rm an empty operand.
	if [ -n "$tmp_prom" ]; then rm -f "$tmp_prom"; fi
	if [ -n "$tmp_state" ]; then rm -f "$tmp_state"; fi
	[ "$published" -eq 1 ] || rm -f "$prom"
}
trap cleanup EXIT
trap 'exit 1' HUP INT TERM
tmp_prom="$(mktemp "$out_dir/.proc-metrics.prom.XXXXXX")"
tmp_state="$(mktemp "$out_dir/.proc-metrics.state.XXXXXX")"
# Final mode before any byte lands; mktemp creates 0600 and Alloy may run as
# a different user than the job that writes the file.
chmod 0644 "$tmp_prom"
chmod 0600 "$tmp_state"

# procps takes UNIX options for "every process"; BSD ps wants -ax.
if [ "$os" = Darwin ]; then
	all=-ax
	kthreads=0
else
	all=-e
	kthreads=1
fi
# Sample first so a failing ps aborts the run instead of publishing an empty file.
# shellcheck disable=SC2086 # PROC_METRICS_PS may carry arguments
sample="$($ps_cmd $all -o pid=,ppid=,time=,rss=,comm=)"
printf '%s\n' "$sample" | awk -v now="$now" -v state="$state" -v prom="$tmp_prom" -v newstate="$tmp_state" -v kthreads="$kthreads" '
# [D-][[H:]M:]S[.f] as printed by ps `time`, in seconds.
function seconds_of(t,    days, n, a, v) {
	days = 0
	if (index(t, "-")) {
		n = split(t, a, "-")
		days = a[1] + 0
		t = a[2]
	}
	n = split(t, a, ":")
	if (n == 3) v = a[1] * 3600 + a[2] * 60 + a[3]
	else if (n == 2) v = a[1] * 60 + a[2]
	else v = a[1] + 0
	return days * 86400 + v
}
# Marks the top `limit` groups by one metric in keep[]. The extra parameters
# after the blank gap are awk locals: `taken` is fresh on every call, so the cpu
# and rss passes each rank over all groups and keep[] is just their union.
function pick(metric, limit, needpositive,    k, name, best, bestv, taken, v) {
	for (k = 0; k < limit; k++) {
		best = ""
		for (name in rss) {
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
		else if (f[1] == "p") { prev_name[f[2]] = f[3]; prev_cpu[f[2]] = f[4] + 0 }
	}
	close(state)
	dt = now - prev_ts
	rebaseline = (prev_ts == 0 || dt < 0 || dt > 120)
}
{
	if (NF < 5) next
	pid = $1
	if (kthreads && (pid == 2 || $2 == 2)) next
	used = seconds_of($3)
	kib = $4 + 0
	name = $0
	sub(/^ *[^ ]+ +[^ ]+ +[^ ]+ +[^ ]+ +/, "", name)
	sub(/.*\//, "", name)
	gsub(/[^A-Za-z0-9._ ()+@:-]/, "_", name)
	sub(/ +$/, "", name)
	name = substr(name, 1, 32)
	if (name == "") name = "unknown"
	# "other" is the fold-in group; keep every real name one-to-one off it.
	if (name ~ /^other_*$/) name = name "_"
	delta = 0
	if (!rebaseline) {
		if (pid in prev_cpu && prev_name[pid] == name && used >= prev_cpu[pid]) delta = used - prev_cpu[pid]
		else delta = used
	}
	cpu[name] += delta
	rss[name] += kib
	procs[name]++
	printf "p\t%s\t%s\t%.2f\n", pid, name, used > newstate
}
END {
	pick("cpu", 15, 1)
	pick("rss", 15, 0)
	for (name in rss) {
		g = keep[name] ? name : "other"
		gcpu[g] += cpu[name]
		grss[g] += rss[name]
		gprocs[g] += procs[name]
	}
	count = 0
	for (g in gprocs) order[++count] = g
	for (i = 2; i <= count; i++) {
		x = order[i]
		for (j = i - 1; j >= 1 && order[j] > x; j--) order[j + 1] = order[j]
		order[j + 1] = x
	}
	printf "ts\t%d\n", now > newstate
	for (i = 1; i <= count; i++) {
		g = order[i]
		sum[g] = total[g] + gcpu[g]
		printf "c\t%s\t%.3f\n", g, sum[g] > newstate
	}
	print "# HELP namedprocess_namegroup_cpu_seconds_total CPU time of the process group, from ps cumulative CPU time deltas." > prom
	print "# TYPE namedprocess_namegroup_cpu_seconds_total counter" > prom
	for (i = 1; i <= count; i++) printf "namedprocess_namegroup_cpu_seconds_total{groupname=\"%s\"} %.3f\n", order[i], sum[order[i]] > prom
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
