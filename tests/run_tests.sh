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

RHEL_2024=$(jq '.result_libdnf[4].count' "${DEST}"/stats.json)
if [ "${RHEL_2024}" != "14" ]; then
	echo "ERROR: json returned unexpected value"
	jq '.' "${DEST}"/stats.json
	exit 1
fi

JP_2023=$(jq '.result_country[4].count' "${DEST}"/stats.json)
if [ "${JP_2023}" != "2" ]; then
	echo "ERROR: json returned unexpected value"
	jq '.' "${DEST}"/stats.json
	exit 1
fi

# OpenHPC 4.x validation tests
OVERALL_2025=$(jq '.unique_visitors_per_year[5].overall' "${DEST}"/stats.json)
if [ "${OVERALL_2025}" != "5" ]; then
	echo "ERROR: OpenHPC 4.x overall count for 2025 returned unexpected value: ${OVERALL_2025}"
	jq '.' "${DEST}"/stats.json
	exit 1
fi

OHPC4_2025=$(jq '.unique_visitors_per_year[5].ohpc4' "${DEST}"/stats.json)
if [ "${OHPC4_2025}" != "5" ]; then
	echo "ERROR: OpenHPC 4.x count for 2025 returned unexpected value: ${OHPC4_2025}"
	jq '.' "${DEST}"/stats.json
	exit 1
fi

# Validate that 2024 has ohpc4 field set to 0 (backward compatibility)
OHPC4_2024=$(jq '.unique_visitors_per_year[4].ohpc4' "${DEST}"/stats.json)
if [ "${OHPC4_2024}" != "0" ]; then
	echo "ERROR: OpenHPC 4.x count for 2024 should be 0, got: ${OHPC4_2024}"
	jq '.' "${DEST}"/stats.json
	exit 1
fi

# Validate distribution detection for OpenHPC 4.x
ROCKY_2025=$(jq '.result_libdnf[] | select(.year == 2025 and .name == "Rocky Linux") | .count' "${DEST}"/stats.json)
if [ "${ROCKY_2025}" != "1" ]; then
	echo "ERROR: Rocky Linux count for 2025 returned unexpected value: ${ROCKY_2025}"
	jq '.' "${DEST}"/stats.json
	exit 1
fi

OPENEULER_2025=$(jq '.result_libdnf[] | select(.year == 2025 and .name == "openEuler") | .count' "${DEST}"/stats.json)
if [ "${OPENEULER_2025}" != "1" ]; then
	echo "ERROR: openEuler count for 2025 returned unexpected value: ${OPENEULER_2025}"
	jq '.' "${DEST}"/stats.json
	exit 1
fi

jq '.' "${DEST}"/stats.json

rm -rf "${DEST}"
echo "PASS"
