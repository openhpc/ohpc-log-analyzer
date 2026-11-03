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

# The --no-svg run above must not have written any SVG files.
if ls "${DEST}"/*.svg >/dev/null 2>&1; then
	echo "ERROR: --no-svg still wrote SVG files"
	ls -la "${DEST}"
	exit 1
fi

# Without --no-svg the four charts have to be written. They are rendered in
# process, so this needs nothing installed on the machine.
SVG_DEST=$(mktemp -d)

"${OHPC_LOG_ANALYZER}" --output-directory "${SVG_DEST}" tests/access_log

for SVG in requests_per_year unique_visitors_per_year \
	requests_per_month unique_visitors_per_month \
	size_per_year size_per_month \
	requests_per_year_and_distribution libdnf_per_year_and_distribution \
	overall_accesses file_types_per_year country_per_month; do
	if [ ! -s "${SVG_DEST}/${SVG}.svg" ]; then
		echo "ERROR: ${SVG}.svg was not written"
		ls -la "${SVG_DEST}"
		exit 1
	fi
	if ! head -n 1 "${SVG_DEST}/${SVG}.svg" |
		grep -q '<svg width="1600" height="600"'; then
		echo "ERROR: ${SVG}.svg is not a 1600x600 SVG"
		head -n 1 "${SVG_DEST}/${SVG}.svg"
		exit 1
	fi
done

# Every chart on the HTML page has to have an SVG of its own. Taking the
# list from the page itself means a chart added later without an SVG fails
# here rather than going unnoticed.
SVG_COUNT=$(find "${SVG_DEST}" -name '*.svg' | wc -l)
TITLE_COUNT=0
while IFS= read -r TITLE; do
	TITLE_COUNT=$((TITLE_COUNT + 1))
	if ! grep -lF "${TITLE}" "${SVG_DEST}"/*.svg >/dev/null; then
		echo "ERROR: no SVG carries the chart title '${TITLE}'"
		exit 1
	fi
done < <(grep -o '"title":{"text":"[^"]*"' "${SVG_DEST}/index.html" |
	sed 's/.*"text":"//; s/"$//' | sort -u)

if [ "${TITLE_COUNT}" -lt 11 ]; then
	echo "ERROR: only ${TITLE_COUNT} chart titles found in the HTML"
	exit 1
fi
# A chart with no data at all writes no SVG, so there can be fewer files
# than charts on a sparse log; more files than charts means a stray one.
if [ "${SVG_COUNT}" -gt "${TITLE_COUNT}" ]; then
	echo "ERROR: ${TITLE_COUNT} charts on the page but ${SVG_COUNT} SVG files"
	ls -la "${SVG_DEST}"
	exit 1
fi

# The overall accesses chart is stacked bars rather than lines, so it has to
# come out as filled rectangles and not as a polyline of one point.
if ! grep -q '<rect' "${SVG_DEST}/overall_accesses.svg"; then
	echo "ERROR: overall_accesses.svg has no bars"
	exit 1
fi

# A file of the right shape is not enough; the series names and the title
# have to reach the rendered output.
for NAME in "OHPC 1.3.x" "OHPC 2.x" "OHPC 3.x" "OHPC 4.x" "Total" \
	"Unique OHPC repository requests per year"; do
	if ! grep -qF "${NAME}" "${SVG_DEST}/unique_visitors_per_year.svg"; then
		echo "ERROR: '${NAME}' missing from unique_visitors_per_year.svg"
		exit 1
	fi
done

# Byte axes are labelled in units rather than in raw digits.
if ! grep -qE '^[0-9.]+ (B|kB|MB|GB|TB|PB)$' "${SVG_DEST}/size_per_year.svg"; then
	echo "ERROR: size_per_year.svg has no unit-formatted axis labels"
	exit 1
fi

# Nothing may be left behind by the temporary files the atomic publish uses.
if ls "${SVG_DEST}"/*.tmp >/dev/null 2>&1; then
	echo "ERROR: a successful run left .tmp files behind"
	ls -la "${SVG_DEST}"
	exit 1
fi

# A run that fails partway through must leave the previously published page
# untouched rather than replacing it with a truncated one. An unreadable
# GeoIP database fails in the last chart, well after the point where the old
# code had already truncated index.html.
GOOD_PAGE=$(sha256sum <"${SVG_DEST}/index.html")

if "${OHPC_LOG_ANALYZER}" --output-directory "${SVG_DEST}" \
	--geoip "${SVG_DEST}/does-not-exist.mmdb" tests/access_log; then
	echo "ERROR: the run with a missing GeoIP database was expected to fail"
	exit 1
fi

if [ "$(sha256sum <"${SVG_DEST}/index.html")" != "${GOOD_PAGE}" ]; then
	echo "ERROR: a failed run damaged the published index.html"
	ls -la "${SVG_DEST}"
	exit 1
fi

if ls "${SVG_DEST}"/*.tmp >/dev/null 2>&1; then
	echo "ERROR: a failed run left .tmp files behind"
	ls -la "${SVG_DEST}"
	exit 1
fi

rm -rf "${SVG_DEST}"

# The per input file counter cache has to be invisible in the results: a run
# that reuses it must produce exactly what a run that parses the log does.
CACHE_DIR=$(mktemp -d)
CACHE_LOG=$(mktemp -d)
COLD=$(mktemp -d)
WARM=$(mktemp -d)
cp tests/access_log "${CACHE_LOG}/access_log"

"${OHPC_LOG_ANALYZER}" --no-svg --cache-directory "${CACHE_DIR}" \
	--output-directory "${COLD}" "${CACHE_LOG}/access_log"
"${OHPC_LOG_ANALYZER}" --no-svg --cache-directory "${CACHE_DIR}" \
	--output-directory "${WARM}" "${CACHE_LOG}/access_log"

if ! diff -q "${DEST}/stats.json" "${COLD}/stats.json"; then
	echo "ERROR: the run that filled the cache disagrees with the plain run"
	exit 1
fi
if ! diff -q "${DEST}/stats.json" "${WARM}/stats.json"; then
	echo "ERROR: the run that reused the cache disagrees with the plain run"
	exit 1
fi

# Appending to a log must invalidate its entry rather than report yesterday's
# numbers for a file that has grown since.
cat tests/access_log >>"${CACHE_LOG}/access_log"
GROWN=$(mktemp -d)
PLAIN_GROWN=$(mktemp -d)
"${OHPC_LOG_ANALYZER}" --no-svg --cache-directory "${CACHE_DIR}" \
	--output-directory "${GROWN}" "${CACHE_LOG}/access_log"
"${OHPC_LOG_ANALYZER}" --no-svg --output-directory "${PLAIN_GROWN}" \
	"${CACHE_LOG}/access_log"

if ! diff -q "${PLAIN_GROWN}/stats.json" "${GROWN}/stats.json"; then
	echo "ERROR: a log that grew was served from a stale cache entry"
	exit 1
fi
if diff -q "${WARM}/stats.json" "${GROWN}/stats.json" >/dev/null; then
	echo "ERROR: the counts did not change although the log doubled"
	exit 1
fi

# A truncated or corrupt entry must cost a reparse, never a wrong answer.
CORRUPT=$(mktemp -d)
for ENTRY in "${CACHE_DIR}"/*.counts; do
	head -c 200 "${ENTRY}" >"${ENTRY}.part"
	mv "${ENTRY}.part" "${ENTRY}"
done
"${OHPC_LOG_ANALYZER}" --no-svg --cache-directory "${CACHE_DIR}" \
	--output-directory "${CORRUPT}" "${CACHE_LOG}/access_log"

if ! diff -q "${PLAIN_GROWN}/stats.json" "${CORRUPT}/stats.json"; then
	echo "ERROR: a corrupt cache entry changed the results"
	exit 1
fi

rm -rf "${CACHE_DIR}" "${CACHE_LOG}" "${COLD}" "${WARM}" "${GROWN}" \
	"${PLAIN_GROWN}" "${CORRUPT}"

jq '.' "${DEST}"/stats.json

rm -rf "${DEST}"
echo "PASS"
