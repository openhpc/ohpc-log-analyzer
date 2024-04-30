#!/bin/bash

set -e -x

if [ -e target/release/ohpc-log-analyzer ]; then
	OHPC_LOG_ANALYZER=target/release/ohpc-log-analyzer
elif [ -e target/debug/ohpc-log-analyzer ]; then
	OHPC_LOG_ANALYZER=target/debug/ohpc-log-analyzer
else
	echo "Binary not found in target/release or target/debug."
	echo "Maybe you need to run 'cargo build --release'."
	exit 1
fi

DEST=$(mktemp -d)

"${OHPC_LOG_ANALYZER}" --no-svg --output-directory "${DEST}" tests/access_log

if [ ! -e "${DEST}/index.html" ]; then
	echo "ERROR: Default output file index.html not found."
	exit 1
fi

OVERALL_2023=$(jq '.unique_visitors_per_year[3].overall' "${DEST}"/stats.json)
if [ "${OVERALL_2023}" != "21" ]; then
	echo "ERROR: json returned unexpected value"
	jq '.' "${DEST}"/stats.json
	exit 1
fi
OVERALL_2024=$(jq '.unique_visitors_per_year[4].overall' "${DEST}"/stats.json)
if [ "${OVERALL_2024}" != "25" ]; then
	echo "ERROR: json returned unexpected value"
	jq '.' "${DEST}"/stats.json
	exit 1
fi

jq '.' "${DEST}"/stats.json

rm -rf "${DEST}"
