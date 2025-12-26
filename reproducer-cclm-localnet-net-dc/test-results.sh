#! /bin/bash
##
## This script read, process and print tests results statistics.
## The script expects the following JSON schema:
## - migrationScope - string, can be "cluster-local" or "cross-cluster"
## - ipFamily - integer, can be '4' or '6'
## - result - float, the test result, e.g.: network dissconection time in seconds.
## - log - string, the test log result was revied from
## Example:
## {
##   "migrationScope": "cluster-local",
##   "ipFamily": 6,
##   "result": 0,
##   "log": ".../test_2010:100:200::10_iperf3.log"
## }
##
## Input JSON are read from $LOGS_PATH, default is "$SCRIPT_PATH/artifacts/*/test-stats.json" 
##
## Acceptable maximal network disconnection time on post migration used is controlled by the
## the env var $MAX_NET_DC_TIME (default is 2, value is seconds count).
##
## Usage example:
##  $ ./test-results.sh 
##
##  $ LOGS_PATH="$HOME/reproducer-cclm-localnet-net-dc/artifacts/*/test-stats.json" ./test-results.sh
##
##  $ export LOGS_PATH="$HOME/artifacts/now/my-test-results.json"
##  $ export MAX_NET_DC_TIME=3
##  $ ./test-results.sh 
##
## Output example:
## cluster-local (multipath)
## {
##   "runs": 2,
##   "pass_rate_percentage": 100,
##   "runs_succeeded": 2,
##   "runs_failed": 0,
##   "max_net_dc_seconds": 0,
##   "min_net_dc_seconds_norm": null,
##   "average_net_dc_seconds_norm": null,
##   "median_net_dc_seconds_norm": null,
##   "runs_failed_info": []
## }
## ...
## cross-cluster (multipath)
## {
##   "runs": 4,
##   "pass_rate_percentage": 75,
##   "runs_succeeded": 3,
##   "runs_failed": 1,
##   "max_net_dc_seconds": 3,
##   "min_net_dc_seconds_norm": 1,
##   "average_net_dc_seconds_norm": 2,
##   "median_net_dc_seconds_norm": 2,
##   "runs_failed_info": [
##     {
##       "migrationScope": "cross-cluster",
##       "ipFamily": 4,
##       "result": 3,
##       "log": "...test_172.31.0.10_iperf3.log"
##     }
##   ]
## }
## ...
## * Fields containing "norm" means calculation was done on non-zero values.
##

set -e

SCRIPT_PATH=$(dirname $(realpath -s $0))

LOGS_PATH="${LOGS_PATH:-${SCRIPT_PATH}/artifacts/*/test-stats.json}"

MAX_NET_DC_TIME=${MAX_NET_DC_TIME:-2}

process(){
  local -r input="$1"
  local -r scope="${2:-}"
  local -r ip_familty=${3:-0}
  
  echo $input | \
  jq --arg s "$scope" --argjson v "$ip_familty" \
    '.[] | if $s != "" then select(.migrationScope == $s) end | 
      if $v > 0        then select(.ipFamily == $v?) end' | \
  jq --argjson m "$MAX_NET_DC_TIME" --slurp \
    '{
      runs: length,
      pass_rate_percentage: (if length == 0 then null else ((map(select(.result <= $m)) | length) / length * 100 | round) end),
      runs_succeeded: (if length == 0 then null else (map(select(.result <= $m)) | length) end),
      runs_failed: (if length == 0 then null else (map(select(.result > $m)) | length) end),
      max_net_dc_seconds: map(.result) | max,
      min_net_dc_seconds_norm: (map(select(.result > 0)) | if length > 0 then (map(.result) | min) else null end),
      average_net_dc_seconds_norm: (map(select(.result > 0)) | if length > 0 then (map(.result) | add / length) else null end),
      median_net_dc_seconds_norm: (map(select(.result > 0)) |  if length > 0 then map(.result) | sort | if length%2==1 then.[length/2|floor]else[.[length/2-1,length/2]]|add/2 end else null end),
      runs_failed_info: (if length == 0 then null else (map(select(.result > $m))) end)
    }'
}

filter_local_migration="cluster-local"
filter_cross_cluser_migration="cross-cluster"
filter_ip_v4="4"
filter_ip_v6="6"

echo "reading logs.."
o=$(ls $LOGS_PATH | xargs cat )

echo "cluster-local (multipath)"
process "$o" "$filter_local_migration"
echo "cluster-local, IPv6"
process "$o" "$filter_local_migration" "$filter_ip_v6"
echo "cluster-local, IPv4"
process "$o" "$filter_local_migration" "$filter_ip_v4"

echo "cross-cluster (multipath)"
process "$o" "$filter_cross_cluser_migration"
echo "cross-cluster, IPv6"
process "$o" "$filter_cross_cluser_migration" "$filter_ip_v6"
echo "cross-cluster, IPv4"
process "$o" "$filter_cross_cluser_migration" "$filter_ip_v4"
