extern crate chrono;
extern crate indicatif;
extern crate serde;

use rustc_hash::FxHashMap;
use std::collections::hash_map::Entry;
use std::fs::File;
use std::io::{
    Read,
    Write,
};
use std::net::{
    IpAddr,
    Ipv4Addr,
    Ipv6Addr,
};
use std::path::Path;
use std::sync::atomic::{
    AtomicUsize,
    Ordering,
};
use std::sync::{
    Arc,
    LazyLock,
    RwLock,
};
use std::time::Instant;
use std::{
    process,
    str,
};

use clap::Parser;
use console::{
    style,
    Emoji,
};
use indicatif::{
    ProgressBar,
    ProgressStyle,
};
use plotly::layout::{
    Axis,
    BarMode,
    Layout,
};
use plotly::{
    Bar,
    ImageFormat,
    Plot,
    Scatter,
};
use memchr::memmem::Finder;
use rayon::prelude::*;
use serde::Serialize;

// Substring searches on every log line are the hot path, so the patterns are
// compiled once into SIMD-accelerated finders instead of being scanned with
// slice::windows(), which compares byte by byte.
static F_GET: LazyLock<Finder> = LazyLock::new(|| Finder::new(b"GET"));
static F_PNG: LazyLock<Finder> = LazyLock::new(|| Finder::new(b".png"));
static F_GIF: LazyLock<Finder> = LazyLock::new(|| Finder::new(b".gif"));
static F_OHPC_1_3: LazyLock<Finder> = LazyLock::new(|| Finder::new(b"/ohpc-1.3/"));
static F_OPENHPC: LazyLock<Finder> = LazyLock::new(|| Finder::new(b"/OpenHPC/"));
static F_SLE_12: LazyLock<Finder> = LazyLock::new(|| Finder::new(b"SLE_12"));
static F_CENTOS_7: LazyLock<Finder> = LazyLock::new(|| Finder::new(b"CentOS_7"));
static F_CENTOS_8: LazyLock<Finder> = LazyLock::new(|| Finder::new(b"CentOS_8"));
static F_LEAP_15: LazyLock<Finder> = LazyLock::new(|| Finder::new(b"Leap_15"));
static F_EL_8: LazyLock<Finder> = LazyLock::new(|| Finder::new(b"EL_8"));
static F_EL_9: LazyLock<Finder> = LazyLock::new(|| Finder::new(b"EL_9"));
static F_EL_10: LazyLock<Finder> = LazyLock::new(|| Finder::new(b"EL_10"));
static F_OPENEULER_22: LazyLock<Finder> = LazyLock::new(|| Finder::new(b"openEuler_22.03"));
static F_OPENEULER_24: LazyLock<Finder> = LazyLock::new(|| Finder::new(b"openEuler_24.03"));

static OVERALL: AtomicUsize = AtomicUsize::new(0);
static OHPC_1: AtomicUsize = AtomicUsize::new(0);
static OHPC_2: AtomicUsize = AtomicUsize::new(0);
static OHPC_3: AtomicUsize = AtomicUsize::new(0);
static OHPC_4: AtomicUsize = AtomicUsize::new(0);
static STEPS: AtomicUsize = AtomicUsize::new(3);
static CALL_COUNT: AtomicUsize = AtomicUsize::new(1);
static CHUNK: AtomicUsize = AtomicUsize::new(0);
/// Chunks handed to the pool but not yet merged. The reader runs ahead of
/// the workers and this is what stops it pulling the whole file into memory.
static CHUNKS_IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Name of the HTML output file
    #[arg(long, default_value = "index.html")]
    html_output: String,

    /// Name of the HTML output file
    #[arg(long, default_value = "/stats")]
    output_directory: String,

    /// Do not write svg output files
    #[arg(long, default_value = "false")]
    no_svg: bool,

    /// GeoIP mmdb database
    #[arg(long, default_value = "/usr/share/GeoIP/GeoLite2-Country.mmdb")]
    geoip: String,

    /// One or multiple access logs
    access_log: Vec<String>,
}

#[derive(Debug, Default)]
struct ResultOverall {
    year: i64,
    ohpc_1: i64,
    ohpc_2: i64,
    ohpc_3: i64,
    ohpc_4: i64,
    overall: i64,
    unique_ohpc_1: i64,
    unique_ohpc_2: i64,
    unique_ohpc_3: i64,
    unique_ohpc_4: i64,
    unique_overall: i64,
    size: u64,
    ipv4: FxHashMap<u32, u8>,
    ipv6: FxHashMap<u128, u8>,
}

#[derive(Debug, Default)]
struct ResultOverallPerMonth {
    year: i64,
    month: i64,
    ohpc_1: i64,
    ohpc_2: i64,
    ohpc_3: i64,
    ohpc_4: i64,
    overall: i64,
    unique_ohpc_1: i64,
    unique_ohpc_2: i64,
    unique_ohpc_3: i64,
    unique_ohpc_4: i64,
    unique_overall: i64,
    size: u64,
    ipv4: FxHashMap<u32, IpStats>,
    ipv6: FxHashMap<u128, IpStats>,
}

#[derive(Debug)]
struct ResultOHPC1 {
    year: i64,
    sles: i64,
    rhel: i64,
}

#[derive(Debug)]
struct ResultOHPC2 {
    year: i64,
    sles: i64,
    rhel: i64,
}

#[derive(Debug)]
struct ResultOHPC3 {
    year: i64,
    sles: i64,
    rhel: i64,
    openeuler: i64,
}

#[derive(Debug)]
struct ResultOHPC4 {
    year: i64,
    rhel: i64,
    openeuler: i64,
}

#[derive(Debug, Serialize)]
struct ResultLIBDNF {
    year: i64,
    name: String,
    count: i64,
}

#[derive(Debug, Serialize)]
struct ResultCountry {
    year: i64,
    month: i64,
    country: String,
    count: i64,
}

#[derive(Debug)]
struct ResultType {
    year: i64,
    tar: i64,
    rpm: i64,
    repomd_xml: i64,
}

#[derive(Serialize)]
struct UniqueVisitorsPerYear {
    year: i64,
    ohpc1: i64,
    ohpc2: i64,
    ohpc3: i64,
    ohpc4: i64,
    overall: i64,
}
#[derive(Serialize)]
struct UniqueVisitorsPerMonth {
    year_month: String,
    ohpc1: i64,
    ohpc2: i64,
    ohpc3: i64,
    ohpc4: i64,
    overall: i64,
}
#[derive(Serialize)]
struct SizePerYear {
    year: i64,
    size: u64,
}
#[derive(Serialize)]
struct SizePerMonth {
    year_month: String,
    size: u64,
}

#[derive(Serialize)]
struct Json {
    unique_visitors_per_year: Vec<UniqueVisitorsPerYear>,
    unique_visitors_per_month: Vec<UniqueVisitorsPerMonth>,
    size_per_year: Vec<SizePerYear>,
    size_per_month: Vec<SizePerMonth>,
    result_libdnf: Vec<ResultLIBDNF>,
    result_country: Vec<ResultCountry>,
}

static OVERALL_RESULTS: RwLock<Vec<ResultOverall>> = RwLock::new(Vec::new());
static OVERALL_RESULTS_PER_MONTH: RwLock<Vec<ResultOverallPerMonth>> = RwLock::new(Vec::new());
static OHPC1_RESULTS: RwLock<Vec<ResultOHPC1>> = RwLock::new(Vec::new());
static OHPC2_RESULTS: RwLock<Vec<ResultOHPC2>> = RwLock::new(Vec::new());
static OHPC3_RESULTS: RwLock<Vec<ResultOHPC3>> = RwLock::new(Vec::new());
static OHPC4_RESULTS: RwLock<Vec<ResultOHPC4>> = RwLock::new(Vec::new());
static LIBDNF_RESULTS: RwLock<Vec<ResultLIBDNF>> = RwLock::new(Vec::new());
static COUNTRY_RESULTS: RwLock<Vec<ResultCountry>> = RwLock::new(Vec::new());
static TYPE_RESULTS: RwLock<Vec<ResultType>> = RwLock::new(Vec::new());

static HTML_HEADER: &str =
    "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\" /></head><body>   <div>
<script src=\"https://cdn.jsdelivr.net/npm/mathjax@3.2.2/es5/tex-svg.js\"></script>
<script src=\"https://cdn.plot.ly/plotly-2.12.1.min.js\"></script>";
static HTML_FOOTER: &str = "</body></html>";

fn last_newline(s: &[u8]) -> usize {
    let mut i = s.len() - 1;
    while i > 0 {
        if s[i] == b'\n' {
            return i + 1;
        }
        i -= 1;
    }
    s.len()
}

/// Release membership of a single client address, one bit per OpenHPC
/// release, so that a client counts as a unique user of every release it
/// actually requested.
const RELEASE_BITS: [u8; 4] = [1, 2, 4, 8];

/// What one client address did within one year-month.
#[derive(Debug, Default, Clone, Copy)]
struct IpStats {
    requests: i64,
    releases: u8,
}

#[derive(Debug, Default)]
struct YearCounts {
    ohpc: [i64; 4],
    overall: i64,
    size: u64,
    ipv4: FxHashMap<u32, u8>,
    ipv6: FxHashMap<u128, u8>,
}

#[derive(Debug, Default)]
struct MonthCounts {
    ohpc: [i64; 4],
    overall: i64,
    size: u64,
    ipv4: FxHashMap<u32, IpStats>,
    ipv6: FxHashMap<u128, IpStats>,
}

/// Everything one chunk of the input learned. Each rayon task fills its own
/// and merges it into the globals once, so the hot loop takes no locks.
#[derive(Debug, Default)]
struct ChunkCounts {
    lines: usize,
    release_lines: [usize; 4],
    years: FxHashMap<i64, YearCounts>,
    months: FxHashMap<(i64, i64), MonthCounts>,
    ohpc1: FxHashMap<i64, [i64; 2]>,
    ohpc2: FxHashMap<i64, [i64; 2]>,
    ohpc3: FxHashMap<i64, [i64; 3]>,
    ohpc4: FxHashMap<i64, [i64; 2]>,
    libdnf: FxHashMap<String, FxHashMap<i64, i64>>,
    types: FxHashMap<i64, [i64; 3]>,
}

/// Index of the entry for `year`, appending a fresh one when there is none.
fn year_slot<T, F>(data: &mut Vec<T>, year: i64, key: fn(&T) -> i64, new: F) -> usize
where
    F: FnOnce() -> T,
{
    match data.iter().position(|r| key(r) == year) {
        Some(i) => i,
        _ => {
            data.push(new());
            data.len() - 1
        }
    }
}

fn merge_chunk(counts: ChunkCounts) {
    OVERALL.fetch_add(counts.lines, Ordering::Relaxed);
    OHPC_1.fetch_add(counts.release_lines[0], Ordering::Relaxed);
    OHPC_2.fetch_add(counts.release_lines[1], Ordering::Relaxed);
    OHPC_3.fetch_add(counts.release_lines[2], Ordering::Relaxed);
    OHPC_4.fetch_add(counts.release_lines[3], Ordering::Relaxed);

    {
        let mut data = OVERALL_RESULTS.write().unwrap();
        for (year, y) in counts.years {
            let i = year_slot(&mut data, year, |r| r.year, || ResultOverall {
                year,
                ..Default::default()
            });
            let entry = &mut data[i];
            entry.overall += y.overall;
            entry.size += y.size;
            entry.ohpc_1 += y.ohpc[0];
            entry.ohpc_2 += y.ohpc[1];
            entry.ohpc_3 += y.ohpc[2];
            entry.ohpc_4 += y.ohpc[3];
            for (ip, releases) in y.ipv4 {
                *entry.ipv4.entry(ip).or_insert(0) |= releases;
            }
            for (ip, releases) in y.ipv6 {
                *entry.ipv6.entry(ip).or_insert(0) |= releases;
            }
        }
    }

    {
        let mut data = OVERALL_RESULTS_PER_MONTH.write().unwrap();
        for ((year, month), m) in counts.months {
            let i = match data.iter().position(|r| r.year == year && r.month == month) {
                Some(i) => i,
                _ => {
                    data.push(ResultOverallPerMonth {
                        year,
                        month,
                        ..Default::default()
                    });
                    data.len() - 1
                }
            };
            let entry = &mut data[i];
            entry.overall += m.overall;
            entry.size += m.size;
            entry.ohpc_1 += m.ohpc[0];
            entry.ohpc_2 += m.ohpc[1];
            entry.ohpc_3 += m.ohpc[2];
            entry.ohpc_4 += m.ohpc[3];
            for (ip, stats) in m.ipv4 {
                let slot = entry.ipv4.entry(ip).or_default();
                slot.requests += stats.requests;
                slot.releases |= stats.releases;
            }
            for (ip, stats) in m.ipv6 {
                let slot = entry.ipv6.entry(ip).or_default();
                slot.requests += stats.requests;
                slot.releases |= stats.releases;
            }
        }
    }

    {
        let mut data = OHPC1_RESULTS.write().unwrap();
        for (year, c) in counts.ohpc1 {
            let i = year_slot(&mut data, year, |r| r.year, || ResultOHPC1 {
                year,
                sles: 0,
                rhel: 0,
            });
            data[i].sles += c[0];
            data[i].rhel += c[1];
        }
    }
    {
        let mut data = OHPC2_RESULTS.write().unwrap();
        for (year, c) in counts.ohpc2 {
            let i = year_slot(&mut data, year, |r| r.year, || ResultOHPC2 {
                year,
                sles: 0,
                rhel: 0,
            });
            data[i].sles += c[0];
            data[i].rhel += c[1];
        }
    }
    {
        let mut data = OHPC3_RESULTS.write().unwrap();
        for (year, c) in counts.ohpc3 {
            let i = year_slot(&mut data, year, |r| r.year, || ResultOHPC3 {
                year,
                sles: 0,
                rhel: 0,
                openeuler: 0,
            });
            data[i].sles += c[0];
            data[i].rhel += c[1];
            data[i].openeuler += c[2];
        }
    }
    {
        let mut data = OHPC4_RESULTS.write().unwrap();
        for (year, c) in counts.ohpc4 {
            let i = year_slot(&mut data, year, |r| r.year, || ResultOHPC4 {
                year,
                rhel: 0,
                openeuler: 0,
            });
            data[i].rhel += c[0];
            data[i].openeuler += c[1];
        }
    }
    {
        let mut data = TYPE_RESULTS.write().unwrap();
        for (year, c) in counts.types {
            let i = year_slot(&mut data, year, |r| r.year, || ResultType {
                year,
                tar: 0,
                rpm: 0,
                repomd_xml: 0,
            });
            data[i].tar += c[0];
            data[i].rpm += c[1];
            data[i].repomd_xml += c[2];
        }
    }
    {
        let mut data = LIBDNF_RESULTS.write().unwrap();
        for (name, per_year) in counts.libdnf {
            for (year, count) in per_year {
                match data
                    .iter_mut()
                    .find(|r| r.year == year && r.name == name)
                {
                    Some(r) => r.count += count,
                    _ => data.push(ResultLIBDNF {
                        year,
                        name: name.clone(),
                        count,
                    }),
                }
            }
        }
    }
}

/// Unique visitor counts are derived once, after all chunks are merged, so
/// they do not depend on the order the chunks happened to finish in.
fn finalize_unique_counts() -> Result<(), Box<dyn std::error::Error>> {
    let mut data = OVERALL_RESULTS.write()?;
    for result in data.iter_mut() {
        let mut unique = [0i64; 4];
        for releases in result.ipv4.values().chain(result.ipv6.values()) {
            for (i, bit) in RELEASE_BITS.iter().enumerate() {
                if releases & bit != 0 {
                    unique[i] += 1;
                }
            }
        }
        result.unique_overall = (result.ipv4.len() + result.ipv6.len()) as i64;
        result.unique_ohpc_1 = unique[0];
        result.unique_ohpc_2 = unique[1];
        result.unique_ohpc_3 = unique[2];
        result.unique_ohpc_4 = unique[3];
    }
    drop(data);

    let mut data = OVERALL_RESULTS_PER_MONTH.write()?;
    for result in data.iter_mut() {
        let mut unique = [0i64; 4];
        for stats in result.ipv4.values().chain(result.ipv6.values()) {
            for (i, bit) in RELEASE_BITS.iter().enumerate() {
                if stats.releases & bit != 0 {
                    unique[i] += 1;
                }
            }
        }
        result.unique_overall = (result.ipv4.len() + result.ipv6.len()) as i64;
        result.unique_ohpc_1 = unique[0];
        result.unique_ohpc_2 = unique[1];
        result.unique_ohpc_3 = unique[2];
        result.unique_ohpc_4 = unique[3];
    }

    Ok(())
}

fn count_type(elements: &[&str], year: i64, counts: &mut ChunkCounts) {
    if elements.len() < 7 {
        return;
    }
    let rpm_found = elements[6].ends_with(".rpm");
    let tar_found = elements[6].ends_with(".tar");
    let repomd_xml_found = elements[6].ends_with("/repomd.xml");
    if !tar_found && !rpm_found && !repomd_xml_found {
        return;
    }
    let entry = counts.types.entry(year).or_default();
    if tar_found {
        entry[0] += 1;
    }
    if rpm_found {
        entry[1] += 1;
    }
    if repomd_xml_found {
        entry[2] += 1;
    }
}

fn count_libdnf(elements: &[&str], year: i64, counts: &mut ChunkCounts) {
    if elements.len() < 12 {
        return;
    }
    if elements[11] != "\"libdnf" {
        return;
    }

    // The distribution name is the first ';'-delimited field of the user
    // agent, minus the trailing version number and the leading '(' -- so
    // "(AlmaLinux 8.9; generic; ..." becomes "AlmaLinux". Collect the words
    // of that field rather than joining the whole tail into a String first.
    let mut words: Vec<&str> = Vec::with_capacity(4);
    for element in &elements[12..] {
        match element.find(';') {
            Some(i) => {
                words.push(&element[..i]);
                break;
            }
            _ => words.push(element),
        }
    }
    // Drop the trailing version number. Without it there is no name to count.
    if words.len() < 2 {
        return;
    }
    words.pop();
    let mut user_agent_short = String::with_capacity(32);
    for (i, word) in words.iter().enumerate() {
        if i > 0 {
            user_agent_short.push(' ');
        }
        user_agent_short.push_str(word);
    }
    // Drop the leading '('. Indexing by byte would split a multi-byte char.
    let user_agent = match user_agent_short.char_indices().nth(1) {
        Some((i, _)) => &user_agent_short[i..],
        _ => return,
    };
    if user_agent.is_empty() {
        return;
    }
    let user_agent = match user_agent {
        "Red Hat Enterprise Linux Server" => "Red Hat Enterprise Linux",
        "CentOS AutoSD" | "CentOS release 8" => "CentOS Linux",
        "RockyLinux" => "Rocky Linux",
        other => other,
    };

    // Only a distribution seen for the first time needs its name allocated.
    match counts.libdnf.get_mut(user_agent) {
        Some(per_year) => *per_year.entry(year).or_insert(0) += 1,
        _ => {
            counts
                .libdnf
                .insert(user_agent.to_string(), FxHashMap::from_iter([(year, 1)]));
        }
    }
}

fn update_distributions_ohpc_3(s: &[u8], year: i64, counts: &mut ChunkCounts) {
    let mut leap_15_found = false;
    let mut el_9_found = false;
    let mut openeuler_found = false;
    if F_LEAP_15.find(s).is_some() {
        leap_15_found = true;
    } else if F_EL_9.find(s).is_some() {
        el_9_found = true;
    } else if F_OPENEULER_22.find(s).is_some() {
        openeuler_found = true;
    }
    if !el_9_found && !leap_15_found && !openeuler_found {
        return;
    }
    let entry = counts.ohpc3.entry(year).or_default();
    if leap_15_found {
        entry[0] += 1;
    }
    if el_9_found {
        entry[1] += 1;
    }
    if openeuler_found {
        entry[2] += 1;
    }
}

fn update_distributions_ohpc_4(s: &[u8], year: i64, counts: &mut ChunkCounts) {
    let mut el_10_found = false;
    let mut openeuler_found = false;
    if F_EL_10.find(s).is_some() {
        el_10_found = true;
    } else if F_OPENEULER_24.find(s).is_some() {
        openeuler_found = true;
    }
    if !el_10_found && !openeuler_found {
        return;
    }
    let entry = counts.ohpc4.entry(year).or_default();
    if el_10_found {
        entry[0] += 1;
    }
    if openeuler_found {
        entry[1] += 1;
    }
}

fn month_to_int(s: &str) -> i64 {
    match s {
        "Jan" => 1,
        "Feb" => 2,
        "Mar" => 3,
        "Apr" => 4,
        "May" => 5,
        "Jun" => 6,
        "Jul" => 7,
        "Aug" => 8,
        "Sep" => 9,
        "Oct" => 10,
        "Nov" => 11,
        "Dec" => 12,
        _ => 0,
    }
}

fn process_line(s: &[u8], counts: &mut ChunkCounts) {
    // Request lines are attacker controlled and regularly contain bytes that
    // are not valid UTF-8; those lines are skipped instead of panicking.
    let line = match str::from_utf8(s) {
        Ok(line) => line,
        _ => return,
    };
    let elements: Vec<&str> = line.split(' ').collect();
    counts.lines += 1;
    // elements[3] holds the timestamp, so four elements are needed.
    if elements.len() < 4 {
        // Skip incomplete lines
        return;
    }
    if elements[3].len() < 8 {
        return;
    }
    let year = match elements[3][8..12].parse::<i64>() {
        Ok(y) => y,
        _ => return,
    };
    let month_string = &elements[3][4..7];
    let month = month_to_int(month_string);
    if month == 0 {
        return;
    }

    count_libdnf(&elements, year, counts);
    count_type(&elements, year, counts);

    let mut releases = 0u8;
    if let Some(search_result) = F_OHPC_1_3.find(s) {
        counts.release_lines[0] += 1;
        releases |= RELEASE_BITS[0];
        let rest = &s[search_result + "/ohpc-1.3/".len()..];
        let entry = counts.ohpc1.entry(year).or_default();
        if F_CENTOS_7.find(rest).is_some() {
            entry[1] += 1;
        } else if F_SLE_12.find(rest).is_some() {
            entry[0] += 1;
        }
    }

    if let Some(search_result) = F_OPENHPC.find(s) {
        let start = search_result + "/OpenHPC/".len();
        // A line truncated right after "/OpenHPC/" has no release digit.
        let release = s.get(start..start + 2).unwrap_or_default();
        if release == "2/".as_bytes() {
            counts.release_lines[1] += 1;
            releases |= RELEASE_BITS[1];
            let rest = &s[start + 2..];
            let entry = counts.ohpc2.entry(year).or_default();
            if F_LEAP_15.find(rest).is_some() {
                entry[0] += 1;
            } else if F_CENTOS_8.find(rest).is_some() || F_EL_8.find(rest).is_some() {
                entry[1] += 1;
            }
        }
        if release == "3/".as_bytes() {
            counts.release_lines[2] += 1;
            releases |= RELEASE_BITS[2];
            update_distributions_ohpc_3(&s[start + 2..], year, counts);
        }
        if release == "4/".as_bytes() {
            counts.release_lines[3] += 1;
            releases |= RELEASE_BITS[3];
            update_distributions_ohpc_4(&s[start + 2..], year, counts);
        }
    }

    let size: u64 = match elements.len() < 10 {
        true => 0,
        false => elements[9].parse::<u64>().unwrap_or_default(),
    };

    let ip = match elements[0].parse::<IpAddr>() {
        Ok(ip) => ip,
        _ => IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
    };

    let year_counts = counts.years.entry(year).or_default();
    year_counts.overall += 1;
    year_counts.size += size;
    for (i, bit) in RELEASE_BITS.iter().enumerate() {
        if releases & bit != 0 {
            year_counts.ohpc[i] += 1;
        }
    }
    match ip {
        IpAddr::V4(ipv4) => *year_counts.ipv4.entry(ipv4.into()).or_insert(0) |= releases,
        IpAddr::V6(ipv6) => *year_counts.ipv6.entry(ipv6.into()).or_insert(0) |= releases,
    }

    let month_counts = counts.months.entry((year, month)).or_default();
    month_counts.overall += 1;
    month_counts.size += size;
    for (i, bit) in RELEASE_BITS.iter().enumerate() {
        if releases & bit != 0 {
            month_counts.ohpc[i] += 1;
        }
    }
    let stats = match ip {
        IpAddr::V4(ipv4) => month_counts.ipv4.entry(ipv4.into()).or_default(),
        IpAddr::V6(ipv6) => month_counts.ipv6.entry(ipv6.into()).or_default(),
    };
    stats.requests += 1;
    stats.releases |= releases;
}

pub fn print_step(msg: String) {
    let s = CALL_COUNT.load(Ordering::SeqCst);
    CALL_COUNT.fetch_add(1, Ordering::SeqCst);
    println!(
        " Step {}: {} {}",
        style(format!("[{}/{}]", s, STEPS.load(Ordering::SeqCst)))
            .bold()
            .dim(),
        Emoji("🔍", ""),
        msg,
    );
}

const CHUNK_SIZE: usize = 4_000_000;

fn main() {
    let start = Instant::now();
    let params = Args::parse();
    let output = Path::new(&params.output_directory).join(&params.html_output);
    STEPS.fetch_add(params.access_log.len(), Ordering::SeqCst);
    print_step(format!(
        "Using '{}' as output directory",
        params.output_directory
    ));
    print_step(format!("Using '{}' as html output", output.display()));
    let spinner_style = ProgressStyle::with_template("{prefix:.bold.dim} {spinner} {wide_msg}")
        .unwrap()
        .tick_chars("⠁⠂⠄⡀⢀⠠⠐⠈ ");
    let pool = rayon::ThreadPoolBuilder::new().build().unwrap();
    let max_in_flight = 2 * pool.current_num_threads();

    for input in params.access_log.clone().into_iter() {
        let pb = ProgressBar::new(0);
        pb.set_style(spinner_style.clone());
        let mut access_log = match std::fs::File::open(input.clone()) {
            Ok(a) => a,
            Err(e) => {
                println!("Opening input file '{input}' failed: {e}");
                process::exit(1);
            }
        };
        print_step(format!("Using '{input:}' as input"));

        pool.in_place_scope(|scope| {
            let mut s = Vec::with_capacity(CHUNK_SIZE);
            loop {
                std::io::Read::by_ref(&mut access_log)
                    .take((CHUNK_SIZE - s.len()) as u64)
                    .read_to_end(&mut s)
                    .unwrap();

                if s.is_empty() {
                    // The file has ended.
                    break;
                }

                CHUNK.fetch_add(CHUNK_SIZE, Ordering::SeqCst);
                // Copy any incomplete lines to the next s.
                let last_newline = last_newline(&s);
                let mut next_s = Vec::with_capacity(CHUNK_SIZE);
                next_s.extend_from_slice(&s[last_newline..]);
                s.truncate(last_newline);
                pb.set_message(format!(
                    "Reading megabytes {}",
                    CHUNK.load(Ordering::SeqCst) / 1024 / 1024
                ));
                pb.inc(1);

                // Keep the reader from running too far ahead of the workers.
                // in_place_scope() runs this loop on the main thread rather
                // than on a pool worker, so waiting here cannot starve the
                // workers of the very thread that has to drain the queue.
                while CHUNKS_IN_FLIGHT.load(Ordering::Acquire) >= max_in_flight {
                    std::thread::yield_now();
                }
                CHUNKS_IN_FLIGHT.fetch_add(1, Ordering::Release);

                // Move our string into a rayon thread.
                let data = s;
                scope.spawn(move |_| {
                    let d_s = data[..last_newline].split(|c| *c == b'\n');
                    let mut counts = ChunkCounts::default();

                    for i in d_s {
                        // Skip images. The dot matters: without it this also
                        // threw away every request for libpng-* and giflib-*.
                        if F_PNG.find(i).is_some() || F_GIF.find(i).is_some() {
                            continue;
                        }
                        if F_GET.find_iter(i).nth(1).is_some() {
                            // Skip broken lines with two or more "GET"s
                            continue;
                        }
                        process_line(i, &mut counts);
                    }
                    merge_chunk(counts);
                    CHUNKS_IN_FLIGHT.fetch_sub(1, Ordering::Release);
                });
                s = next_s;
            }
            pb.finish();
        });
    }
    if let Err(e) = create_plots(params, start) {
        println!("Error creating diagrams: {e}");
        process::exit(1);
    }
}

fn create_overall_plot() -> String {
    let labels = vec!["Accesses"];
    let mut plot = Plot::new();
    plot.add_trace(
        Bar::new(labels.clone(), vec![OHPC_1.load(Ordering::SeqCst)]).name("Release 1.3.x"),
    );
    plot.add_trace(
        Bar::new(labels.clone(), vec![OHPC_2.load(Ordering::SeqCst)]).name("Release 2.x"),
    );
    plot.add_trace(
        Bar::new(labels.clone(), vec![OHPC_3.load(Ordering::SeqCst)]).name("Release 3.x"),
    );
    plot.add_trace(
        Bar::new(labels.clone(), vec![OHPC_4.load(Ordering::SeqCst)]).name("Release 4.x"),
    );
    plot.add_trace(
        Bar::new(
            labels,
            vec![
                OVERALL.load(Ordering::SeqCst)
                    - OHPC_1.load(Ordering::SeqCst)
                    - OHPC_2.load(Ordering::SeqCst)
                    - OHPC_3.load(Ordering::SeqCst)
                    - OHPC_4.load(Ordering::SeqCst),
            ],
        )
        .name("Other"),
    );
    plot.set_layout(
        Layout::new()
            .bar_mode(BarMode::Stack)
            .title("OHPC overall repository accesses"),
    );

    plot.to_inline_html(None)
}

fn create_type_plot() -> String {
    let mut years: Vec<i64> = Vec::new();
    let mut ticks: Vec<f64> = Vec::new();
    let data = TYPE_RESULTS.read().unwrap();
    for result in &*data {
        years.push(result.year);
    }
    years.sort_unstable();

    let mut tar: Vec<i64> = Vec::new();
    let mut rpm: Vec<i64> = Vec::new();
    let mut repomd_xml: Vec<i64> = Vec::new();
    for year in &years {
        for result in &*data {
            if result.year == *year {
                tar.push(result.tar);
                rpm.push(result.rpm);
                repomd_xml.push(result.repomd_xml);
                ticks.push((*year) as f64);
                break;
            }
        }
    }

    let mut plot = Plot::new();
    plot.add_trace(Scatter::new(years.clone(), tar).name("TAR"));
    plot.add_trace(Scatter::new(years.clone(), rpm).name("RPM"));
    plot.add_trace(Scatter::new(years.clone(), repomd_xml).name("repomd.xml"));

    plot.set_layout(
        Layout::new()
            .title("OHPC file types per year")
            .x_axis(Axis::new().tick_values(ticks.clone())),
    );

    plot.to_inline_html(None)
}

fn get_years(years: &mut Vec<i64>) -> Result<(), Box<dyn std::error::Error>> {
    let data = OVERALL_RESULTS.read()?;
    for result in &*data {
        years.push(result.year);
    }
    years.sort_unstable();

    Ok(())
}

fn create_repository_requests_per_year(
    years: &Vec<i64>,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut ohpc_1: Vec<i64> = Vec::new();
    let mut ohpc_2: Vec<i64> = Vec::new();
    let mut ohpc_3: Vec<i64> = Vec::new();
    let mut ohpc_4: Vec<i64> = Vec::new();
    let mut overall: Vec<i64> = Vec::new();
    let mut ticks: Vec<f64> = Vec::new();
    let data = OVERALL_RESULTS.read()?;

    for year in years {
        for result in &*data {
            if result.year == *year {
                ohpc_1.push(result.ohpc_1);
                ohpc_2.push(result.ohpc_2);
                ohpc_3.push(result.ohpc_3);
                ohpc_4.push(result.ohpc_4);
                overall.push(result.overall);
                ticks.push((*year) as f64);
                break;
            }
        }
    }

    let mut plot = Plot::new();
    plot.add_trace(Scatter::new(years.clone(), ohpc_1).name("OHPC 1.3.x"));
    plot.add_trace(Scatter::new(years.clone(), ohpc_2).name("OHPC 2.x"));
    plot.add_trace(Scatter::new(years.clone(), ohpc_3).name("OHPC 3.x"));
    plot.add_trace(Scatter::new(years.clone(), ohpc_4).name("OHPC 4.x"));
    plot.add_trace(Scatter::new(years.clone(), overall).name("Total"));
    plot.set_layout(
        Layout::new()
            .title("OHPC repository requests per year")
            .x_axis(Axis::new().tick_values(ticks.clone())),
    );

    Ok(plot.to_inline_html(None))
}

fn create_unique_repository_requests_per_year(
    years: &Vec<i64>,
    params: &Args,
    json: &mut Json,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut unique_ohpc_1: Vec<i64> = Vec::new();
    let mut unique_ohpc_2: Vec<i64> = Vec::new();
    let mut unique_ohpc_3: Vec<i64> = Vec::new();
    let mut unique_ohpc_4: Vec<i64> = Vec::new();
    let mut unique_overall: Vec<i64> = Vec::new();
    let mut ticks: Vec<f64> = Vec::new();

    let data = OVERALL_RESULTS.read()?;
    for year in years {
        for result in &*data {
            if result.year == *year {
                unique_ohpc_1.push(result.unique_ohpc_1);
                unique_ohpc_2.push(result.unique_ohpc_2);
                unique_ohpc_3.push(result.unique_ohpc_3);
                unique_ohpc_4.push(result.unique_ohpc_4);
                unique_overall.push(result.unique_overall);
                ticks.push((*year) as f64);
                json.unique_visitors_per_year.push(UniqueVisitorsPerYear {
                    year: *year,
                    ohpc1: result.unique_ohpc_1,
                    ohpc2: result.unique_ohpc_2,
                    ohpc3: result.unique_ohpc_3,
                    ohpc4: result.unique_ohpc_4,
                    overall: result.unique_overall,
                });
                break;
            }
        }
    }

    let mut unique_plot = Plot::new();
    let trace_unique_ohpc_1 = Scatter::new(years.clone(), unique_ohpc_1).name("OHPC 1.3.x");
    let trace_unique_ohpc_2 = Scatter::new(years.clone(), unique_ohpc_2).name("OHPC 2.x");
    let trace_unique_ohpc_3 = Scatter::new(years.clone(), unique_ohpc_3).name("OHPC 3.x");
    let trace_unique_ohpc_4 = Scatter::new(years.clone(), unique_ohpc_4).name("OHPC 4.x");
    let trace_unique_overall = Scatter::new(years.clone(), unique_overall).name("Total");
    unique_plot.add_trace(trace_unique_ohpc_1);
    unique_plot.add_trace(trace_unique_ohpc_2);
    unique_plot.add_trace(trace_unique_ohpc_3);
    unique_plot.add_trace(trace_unique_ohpc_4);
    unique_plot.add_trace(trace_unique_overall);
    let unique_layout = Layout::new()
        .title("Unique OHPC repository requests per year")
        .x_axis(Axis::new().tick_values(ticks.clone()));
    unique_plot.set_layout(unique_layout);
    if !params.no_svg {
        unique_plot.write_image(
            Path::new(&params.output_directory).join("unique_visitors_per_year.svg"),
            ImageFormat::SVG,
            1600,
            600,
            1.0,
        );
    }

    Ok(unique_plot.to_inline_html(None))
}

fn create_data_downloaded_per_year(
    years: &Vec<i64>,
    params: &Args,
    json: &mut Json,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut ticks: Vec<f64> = Vec::new();
    let mut size: Vec<u64> = Vec::new();
    let data = OVERALL_RESULTS.read()?;

    for year in years {
        for result in &*data {
            if result.year == *year {
                size.push(result.size);
                ticks.push((*year) as f64);
                json.size_per_year.push(SizePerYear {
                    year: *year,
                    size: result.size,
                });
                break;
            }
        }
    }

    let mut plot_size_per_year = Plot::new();
    let size_per_year = Scatter::new(years.clone(), size).name("Total");
    plot_size_per_year.add_trace(size_per_year);
    let layout_size_per_year = Layout::new()
        .title("OHPC data downloaded per year")
        .x_axis(Axis::new().tick_values(ticks.clone()));
    plot_size_per_year.set_layout(layout_size_per_year);

    if !params.no_svg {
        plot_size_per_year.write_image(
            Path::new(&params.output_directory).join("size_per_year.svg"),
            ImageFormat::SVG,
            1600,
            600,
            1.0,
        );
    }

    Ok(plot_size_per_year.to_inline_html(None))
}

fn create_repository_requests_per_month() -> Result<String, Box<dyn std::error::Error>> {
    let mut ohpc_1_per_month: Vec<i64> = Vec::new();
    let mut ohpc_2_per_month: Vec<i64> = Vec::new();
    let mut ohpc_3_per_month: Vec<i64> = Vec::new();
    let mut ohpc_4_per_month: Vec<i64> = Vec::new();
    let mut overall_per_month: Vec<i64> = Vec::new();
    let mut year_months: Vec<String> = Vec::new();

    let data = OVERALL_RESULTS_PER_MONTH.read()?;
    for result in &*data {
        year_months.push(format!("{}-{:02}", result.year, result.month));
    }
    year_months.sort_unstable();

    for year_month in &year_months {
        for result in &*data {
            if format!("{}-{:02}", result.year, result.month) == *year_month {
                ohpc_1_per_month.push(result.ohpc_1);
                ohpc_2_per_month.push(result.ohpc_2);
                ohpc_3_per_month.push(result.ohpc_3);
                ohpc_4_per_month.push(result.ohpc_4);
                overall_per_month.push(result.overall);
                break;
            }
        }
    }

    let mut plot_overall_per_month = Plot::new();
    plot_overall_per_month
        .add_trace(Scatter::new(year_months.clone(), ohpc_1_per_month).name("OHPC 1.3.x"));
    plot_overall_per_month
        .add_trace(Scatter::new(year_months.clone(), ohpc_2_per_month).name("OHPC 2.x"));
    plot_overall_per_month
        .add_trace(Scatter::new(year_months.clone(), ohpc_3_per_month).name("OHPC 3.x"));
    plot_overall_per_month
        .add_trace(Scatter::new(year_months.clone(), ohpc_4_per_month).name("OHPC 4.x"));
    plot_overall_per_month
        .add_trace(Scatter::new(year_months.clone(), overall_per_month).name("Total"));
    plot_overall_per_month
        .set_layout(Layout::new().title("OHPC repository requests per month"));

    Ok(plot_overall_per_month.to_inline_html(None))
}

fn create_unique_repository_requests_per_month(
    params: &Args,
    json: &mut Json,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut unique_ohpc_1_per_month: Vec<i64> = Vec::new();
    let mut unique_ohpc_2_per_month: Vec<i64> = Vec::new();
    let mut unique_ohpc_3_per_month: Vec<i64> = Vec::new();
    let mut unique_ohpc_4_per_month: Vec<i64> = Vec::new();
    let mut unique_overall_per_month: Vec<i64> = Vec::new();
    let mut year_months: Vec<String> = Vec::new();

    let data = OVERALL_RESULTS_PER_MONTH.read()?;
    for result in &*data {
        year_months.push(format!("{}-{:02}", result.year, result.month));
    }
    year_months.sort_unstable();

    for year_month in &year_months {
        for result in &*data {
            if format!("{}-{:02}", result.year, result.month) == *year_month {
                unique_ohpc_1_per_month.push(result.unique_ohpc_1);
                unique_ohpc_2_per_month.push(result.unique_ohpc_2);
                unique_ohpc_3_per_month.push(result.unique_ohpc_3);
                unique_ohpc_4_per_month.push(result.unique_ohpc_4);
                unique_overall_per_month.push(result.unique_overall);
                json.unique_visitors_per_month.push(UniqueVisitorsPerMonth {
                    year_month: year_month.clone(),
                    ohpc1: result.unique_ohpc_1,
                    ohpc2: result.unique_ohpc_2,
                    ohpc3: result.unique_ohpc_3,
                    ohpc4: result.unique_ohpc_4,
                    overall: result.unique_overall,
                });
                break;
            }
        }
    }

    let mut plot = Plot::new();
    plot.add_trace(Scatter::new(year_months.clone(), unique_ohpc_1_per_month).name("OHPC 1.3.x"));
    plot.add_trace(Scatter::new(year_months.clone(), unique_ohpc_2_per_month).name("OHPC 2.x"));
    plot.add_trace(Scatter::new(year_months.clone(), unique_ohpc_3_per_month).name("OHPC 3.x"));
    plot.add_trace(Scatter::new(year_months.clone(), unique_ohpc_4_per_month).name("OHPC 4.x"));
    plot.add_trace(Scatter::new(year_months.clone(), unique_overall_per_month).name("Total"));
    plot.set_layout(Layout::new().title("Unique OHPC repository requests per month"));

    if !params.no_svg {
        plot.write_image(
            Path::new(&params.output_directory).join("unique_visitors_per_month.svg"),
            ImageFormat::SVG,
            1600,
            600,
            1.0,
        );
    }

    Ok(plot.to_inline_html(None))
}

fn create_repository_requests_per_year_and_distribution(
) -> Result<String, Box<dyn std::error::Error>> {
    let mut ohpc_1_sles: Vec<i64> = Vec::new();
    let mut ohpc_1_rhel: Vec<i64> = Vec::new();
    let data_ohpc_1 = OHPC1_RESULTS.read()?;
    let mut years = Vec::new();
    let mut ticks: Vec<f64> = Vec::new();

    for result in &*data_ohpc_1 {
        years.push(result.year);
    }
    years.sort_unstable();
    for year in &years {
        for result in &*data_ohpc_1 {
            if result.year == *year {
                ohpc_1_sles.push(result.sles);
                ohpc_1_rhel.push(result.rhel);
                ticks.push((*year) as f64);
                break;
            }
        }
    }
    let mut plot = Plot::new();
    plot.add_trace(Scatter::new(years.clone(), ohpc_1_sles).name("OHPC SLES 1.3.x"));
    plot.add_trace(Scatter::new(years.clone(), ohpc_1_rhel).name("OHPC RHEL 1.3.x"));
    plot.set_layout(
        Layout::new()
            .title("OHPC repository requests per year and distribution")
            .x_axis(Axis::new().tick_values(ticks.clone())),
    );

    let mut ohpc_2_sles: Vec<i64> = Vec::new();
    let mut ohpc_2_rhel: Vec<i64> = Vec::new();
    let data_ohpc_2 = OHPC2_RESULTS.read()?;
    years = Vec::new();
    for result in &*data_ohpc_2 {
        years.push(result.year);
    }
    years.sort_unstable();

    for year in &years {
        for result in &*data_ohpc_2 {
            if result.year == *year {
                ohpc_2_sles.push(result.sles);
                ohpc_2_rhel.push(result.rhel);
                break;
            }
        }
    }
    plot.add_trace(Scatter::new(years.clone(), ohpc_2_sles).name("OHPC SLES 2.x"));
    plot.add_trace(Scatter::new(years.clone(), ohpc_2_rhel).name("OHPC RHEL 2.x"));

    let mut ohpc_3_sles: Vec<i64> = Vec::new();
    let mut ohpc_3_rhel: Vec<i64> = Vec::new();
    let mut ohpc_3_openeuler: Vec<i64> = Vec::new();
    let data_ohpc_3 = OHPC3_RESULTS.read().unwrap();
    years = Vec::new();
    for result in &*data_ohpc_3 {
        years.push(result.year);
    }
    years.sort_unstable();
    for year in &years {
        for result in &*data_ohpc_3 {
            if result.year == *year {
                ohpc_3_sles.push(result.sles);
                ohpc_3_rhel.push(result.rhel);
                ohpc_3_openeuler.push(result.openeuler);
                break;
            }
        }
    }
    plot.add_trace(Scatter::new(years.clone(), ohpc_3_sles).name("OHPC SLES 3.x"));
    plot.add_trace(Scatter::new(years.clone(), ohpc_3_rhel).name("OHPC RHEL 3.x"));
    plot.add_trace(Scatter::new(years.clone(), ohpc_3_openeuler).name("OHPC openEuler 3.x"));

    let mut ohpc_4_rhel: Vec<i64> = Vec::new();
    let mut ohpc_4_openeuler: Vec<i64> = Vec::new();
    let data_ohpc_4 = OHPC4_RESULTS.read().unwrap();
    years = Vec::new();
    for result in &*data_ohpc_4 {
        years.push(result.year);
    }
    years.sort_unstable();
    for year in &years {
        for result in &*data_ohpc_4 {
            if result.year == *year {
                ohpc_4_rhel.push(result.rhel);
                ohpc_4_openeuler.push(result.openeuler);
                break;
            }
        }
    }
    plot.add_trace(Scatter::new(years.clone(), ohpc_4_rhel).name("OHPC RHEL 4.x"));
    plot.add_trace(Scatter::new(years.clone(), ohpc_4_openeuler).name("OHPC openEuler 4.x"));

    Ok(plot.to_inline_html(None))
}

fn create_data_downloaded_per_month(
    params: &Args,
    json: &mut Json,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut size_per_month: Vec<u64> = Vec::new();
    let mut year_months: Vec<String> = Vec::new();

    let data = OVERALL_RESULTS_PER_MONTH.read()?;
    for result in &*data {
        year_months.push(format!("{}-{:02}", result.year, result.month));
    }
    year_months.sort_unstable();

    for year_month in &year_months {
        for result in &*data {
            if format!("{}-{:02}", result.year, result.month) == *year_month {
                size_per_month.push(result.size);
                json.size_per_month.push(SizePerMonth {
                    year_month: year_month.clone(),
                    size: result.size,
                });
                break;
            }
        }
    }

    let mut plot = Plot::new();
    plot.add_trace(Scatter::new(year_months.clone(), size_per_month).name("Total"));
    plot.set_layout(Layout::new().title("OHPC data downloaded per month"));

    if !params.no_svg {
        plot.write_image(
            Path::new(&params.output_directory).join("size_per_month.svg"),
            ImageFormat::SVG,
            1600,
            600,
            1.0,
        );
    }

    Ok(plot.to_inline_html(None))
}

fn fill_country_results(params: &Args) -> Result<(), Box<dyn std::error::Error>> {
    let geoip_reader = match maxminddb::Reader::open_readfile(&params.geoip) {
        Ok(geoip_reader) => Arc::new(geoip_reader),
        _ => {
            println!("Reading GeoIP2 database {} failed", params.geoip);
            process::exit(1);
        }
    };

    // maxminddb::geoip2::Country deserializes the whole record, including the
    // continent and every localized name, just to reach country.iso_code.
    // Decoding only the one field that is used skips all of that.
    #[derive(serde::Deserialize)]
    struct IsoCode<'a> {
        iso_code: Option<&'a str>,
    }
    #[derive(serde::Deserialize)]
    struct CountryRecord<'a> {
        #[serde(borrow)]
        country: Option<IsoCode<'a>>,
    }

    fn lookup(reader: &maxminddb::Reader<Vec<u8>>, ip: IpAddr) -> String {
        match reader.lookup(ip).and_then(|r| r.decode::<CountryRecord>()) {
            Ok(Some(c)) => c
                .country
                .and_then(|c| c.iso_code)
                .map_or("N/A".to_string(), |iso| iso.to_string()),
            _ => "N/A".to_string(),
        }
    }

    fn merge(mut a: FxHashMap<String, i64>, b: FxHashMap<String, i64>) -> FxHashMap<String, i64> {
        for (k, v) in b {
            *a.entry(k).or_insert(0) += v;
        }
        a
    }

    let data = OVERALL_RESULTS_PER_MONTH.read()?;
    let mut country_results = COUNTRY_RESULTS.write().unwrap();

    for result in &*data {
        let v4 = result
            .ipv4
            .par_iter()
            .fold(FxHashMap::default, |mut acc, (key, stats)| {
                let c = lookup(&geoip_reader, IpAddr::V4(Ipv4Addr::from(*key)));
                *acc.entry(c).or_insert(0) += stats.requests;
                acc
            })
            .reduce(FxHashMap::default, merge);
        let v6 = result
            .ipv6
            .par_iter()
            .fold(FxHashMap::default, |mut acc, (key, stats)| {
                let c = lookup(&geoip_reader, IpAddr::V6(Ipv6Addr::from(*key)));
                *acc.entry(c).or_insert(0) += stats.requests;
                acc
            })
            .reduce(FxHashMap::default, merge);

        for (country, count) in merge(v4, v6) {
            country_results.push(ResultCountry {
                year: result.year,
                month: result.month,
                country,
                count,
            });
        }
    }

    Ok(())
}

fn create_country_per_year_and_month(
    params: &Args,
    json: &mut Json,
) -> Result<String, Box<dyn std::error::Error>> {
    fill_country_results(params)?;

    #[derive(Debug, Default)]
    struct CountryTraceResults {
        year_months: Vec<String>,
        count: Vec<i64>,
    }

    let data = COUNTRY_RESULTS.read()?;

    // Sort row indices by (country, year, month) once instead of scanning the
    // whole table for every (country, year_month) combination.
    let mut order: Vec<usize> = (0..data.len()).collect();
    order.sort_unstable_by(|a, b| {
        let (x, y) = (&data[*a], &data[*b]);
        x.country
            .cmp(&y.country)
            .then(x.year.cmp(&y.year))
            .then(x.month.cmp(&y.month))
    });

    let mut country_results: FxHashMap<String, CountryTraceResults> = FxHashMap::default();
    for i in order {
        let result = &data[i];
        let year_month = format!("{}-{:02}", result.year, result.month);
        let values = country_results.entry(result.country.clone()).or_default();
        values.year_months.push(year_month);
        values.count.push(result.count);
        json.result_country.push(ResultCountry {
            year: result.year,
            month: result.month,
            country: result.country.clone(),
            count: result.count,
        });
    }

    let mut plot = Plot::new();
    let layout_country = Layout::new().title("OHPC repository country requests per month");
    plot.set_layout(layout_country);

    let mut names: Vec<&String> = country_results.keys().collect();
    names.sort_unstable();
    for result in names {
        let trace_countries = Scatter::new(
            country_results[result].year_months.clone(),
            country_results[result].count.clone(),
        )
        .name(result);
        plot.add_trace(trace_countries);
    }

    Ok(plot.to_inline_html(None))
}

fn create_libdnf_requests_per_year_and_distribution(
    json: &mut Json,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut ticks: Vec<f64> = Vec::new();
    let data_libdnf = LIBDNF_RESULTS.read()?;
    let mut years = Vec::new();
    let mut distributions: Vec<String> = Vec::new();

    #[derive(Debug)]
    struct LibdnfTraceResults {
        years: Vec<i64>,
        count: Vec<i64>,
    }

    for result in &*data_libdnf {
        years.push(result.year);
        distributions.push(result.name.clone());
    }
    years.sort_unstable();
    years.dedup();
    distributions.sort_unstable();
    distributions.dedup();

    for year in years.clone() {
        for result in &*data_libdnf {
            if result.year == year {
                ticks.push((year) as f64);
                break;
            }
        }
    }

    let mut libdnf_results: FxHashMap<String, LibdnfTraceResults> = FxHashMap::default();

    for distribution in &distributions {
        for year in &years {
            for result in &*data_libdnf {
                if result.year == *year && result.name == *distribution {
                    let values = match libdnf_results.entry((*distribution.clone()).to_string()) {
                        Entry::Occupied(o) => o.into_mut(),
                        Entry::Vacant(v) => v.insert(LibdnfTraceResults {
                            years: Vec::new(),
                            count: Vec::new(),
                        }),
                    };
                    values.years.push(*year);
                    values.count.push(result.count);
                    json.result_libdnf.push(ResultLIBDNF {
                        year: *year,
                        name: result.name.clone(),
                        count: result.count,
                    });
                }
            }
        }
    }

    let mut plot_libdnf = Plot::new();
    let layout_libdnf = Layout::new()
        .title("OHPC repository libdnf requests per year and distribution")
        .x_axis(Axis::new().tick_values(ticks));
    plot_libdnf.set_layout(layout_libdnf);

    for result in libdnf_results.keys() {
        let trace_libdnf = Scatter::new(
            libdnf_results[result].years.clone(),
            libdnf_results[result].count.clone(),
        )
        .name(result);
        plot_libdnf.add_trace(trace_libdnf);
    }

    Ok(plot_libdnf.to_inline_html(None))
}

fn create_plots(params: Args, start: Instant) -> Result<(), Box<dyn std::error::Error>> {
    finalize_unique_counts()?;

    let mut years: Vec<i64> = Vec::new();
    get_years(&mut years)?;

    let mut json = Json {
        unique_visitors_per_year: Vec::new(),
        unique_visitors_per_month: Vec::new(),
        size_per_year: Vec::new(),
        size_per_month: Vec::new(),
        result_libdnf: Vec::new(),
        result_country: Vec::new(),
    };

    let mut file = File::create(Path::new(&params.output_directory).join(&params.html_output))?;
    file.write_all(HTML_HEADER.as_bytes())?;
    file.write_all(create_repository_requests_per_year(&years)?.as_bytes())?;
    file.write_all(
        create_unique_repository_requests_per_year(&years, &params, &mut json)?.as_bytes(),
    )?;
    file.write_all(create_repository_requests_per_month()?.as_bytes())?;
    file.write_all(create_unique_repository_requests_per_month(&params, &mut json)?.as_bytes())?;
    file.write_all(create_data_downloaded_per_year(&years, &params, &mut json)?.as_bytes())?;
    file.write_all(create_data_downloaded_per_month(&params, &mut json)?.as_bytes())?;
    file.write_all(create_repository_requests_per_year_and_distribution()?.as_bytes())?;
    file.write_all(create_libdnf_requests_per_year_and_distribution(&mut json)?.as_bytes())?;
    file.write_all(create_overall_plot().as_bytes())?;
    file.write_all(create_type_plot().as_bytes())?;
    file.write_all(create_country_per_year_and_month(&params, &mut json)?.as_bytes())?;
    file.write_all("</div>".as_bytes())?;
    let duration = start.elapsed();
    let timestamp = chrono::Utc::now().to_rfc2822();
    file.write_all(
        format!(
            "<center>Generated on {timestamp}. Runtime: {duration:?}</center>"
        )
        .as_bytes(),
    )?;
    file.write_all(HTML_FOOTER.as_bytes())?;

    let mut writer = std::io::BufWriter::new(File::create(
        Path::new(&params.output_directory).join("stats.json"),
    )?);
    serde_json::to_writer(&mut writer, &json)?;
    writer.flush()?;

    print_step(format!("Finished after {duration:?} at {timestamp}"));

    Ok(())
}
