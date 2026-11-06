extern crate chrono;
extern crate indicatif;
extern crate serde;

use rustc_hash::FxHashMap;
use std::collections::hash_map::Entry;
use std::fs::{self, File};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::io::{
    Read,
    Write,
};
use std::net::{
    IpAddr,
    Ipv4Addr,
    Ipv6Addr,
};
use std::path::{Path, PathBuf};
use std::sync::atomic::{
    AtomicUsize,
    Ordering,
};
use std::sync::{
    Arc,
    LazyLock,
    Mutex,
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
use memchr::memmem::Finder;
use plotly::layout::{
    Axis,
    BarMode,
    Layout,
};
use plotly::{
    Bar,
    Plot,
    Scatter,
};
use plotters::prelude::*;
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

    /// Directory holding the per input file counter cache. When it is set,
    /// the counts for each input file are stored there and reused for as
    /// long as that file is unchanged, so a rotated log is parsed only once.
    #[arg(long)]
    cache_directory: Option<String>,

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

// site.css, favicon.ico and the logo are installed under /repos by the
// openhpc-infrastructure Ansible role, and /stats is served by the same
// Apache instance, so the absolute paths below resolve on the live site.
static HTML_HEADER: &str = r#"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0" />
<meta name="description" content="Download statistics of the OpenHPC package repositories" />
<title>OpenHPC Download Statistics</title>
<link rel="icon" type="image/x-icon" href="/.files/favicon.ico" />
<link rel="stylesheet" type="text/css" href="/.files/site.css" />
</head>
<body>
<header class="site-header">
<div class="site-header-inner">
<a class="site-logo" href="/">
<img src="/HEADER.images/ohpc_logo.png" alt="OpenHPC" />
</a>
<nav class="site-nav" aria-label="OpenHPC links">
<a href="https://openhpc.community/">openhpc.community</a>
<a href="https://github.com/openhpc/ohpc">GitHub</a>
<a href="https://lists.openhpc.community/">Mailing Lists</a>
<a href="https://github.com/openhpc/ohpc/wiki/System-Registry">System Registry</a>
<a href="/results/">Test Results</a>
<a href="/stats/">Statistics</a>
</nav>
</div>
</header>
<div>
<script src="https://cdn.jsdelivr.net/npm/mathjax@3.2.2/es5/tex-svg.js"></script>
<script src="https://cdn.plot.ly/plotly-2.12.1.min.js"></script>"#;
// @@GENERATED@@ is replaced at run time, in the same way the Ansible role
// fills @@VERSION@@ in HEADER.tmpl.
static HTML_FOOTER: &str = r#"
<footer class="site-footer">
<div>Generated on @@GENERATED@@</div>
<a href="https://openhpc.community/">OpenHPC</a> package repository &middot;
<a href="/results/">Test Results</a> &middot;
<a href="/stats/">Download Statistics</a> &middot;
<a href="https://lists.openhpc.community/">Mailing Lists</a>
</footer>
</body></html>"#;

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

// Everything a chunk contributes is already collected in ChunkCounts, and
// merging two of them is associative, so a whole input file can be folded
// into a single ChunkCounts and kept on disk. A log file that has been
// rotated never changes again, which means the 33 GB of historical logs are
// parsed once and only read back as counters afterwards.
fn merge_counts(dst: &mut ChunkCounts, src: ChunkCounts) {
    dst.lines += src.lines;
    for (i, v) in src.release_lines.iter().enumerate() {
        dst.release_lines[i] += v;
    }

    for (year, y) in src.years {
        let e = dst.years.entry(year).or_default();
        e.overall += y.overall;
        e.size += y.size;
        for (i, v) in y.ohpc.iter().enumerate() {
            e.ohpc[i] += v;
        }
        for (ip, releases) in y.ipv4 {
            *e.ipv4.entry(ip).or_insert(0) |= releases;
        }
        for (ip, releases) in y.ipv6 {
            *e.ipv6.entry(ip).or_insert(0) |= releases;
        }
    }

    for (key, m) in src.months {
        let e = dst.months.entry(key).or_default();
        e.overall += m.overall;
        e.size += m.size;
        for (i, v) in m.ohpc.iter().enumerate() {
            e.ohpc[i] += v;
        }
        for (ip, stats) in m.ipv4 {
            let slot = e.ipv4.entry(ip).or_default();
            slot.requests += stats.requests;
            slot.releases |= stats.releases;
        }
        for (ip, stats) in m.ipv6 {
            let slot = e.ipv6.entry(ip).or_default();
            slot.requests += stats.requests;
            slot.releases |= stats.releases;
        }
    }

    for (year, c) in src.ohpc1 {
        let e = dst.ohpc1.entry(year).or_default();
        for (i, v) in c.iter().enumerate() {
            e[i] += v;
        }
    }
    for (year, c) in src.ohpc2 {
        let e = dst.ohpc2.entry(year).or_default();
        for (i, v) in c.iter().enumerate() {
            e[i] += v;
        }
    }
    for (year, c) in src.ohpc3 {
        let e = dst.ohpc3.entry(year).or_default();
        for (i, v) in c.iter().enumerate() {
            e[i] += v;
        }
    }
    for (year, c) in src.ohpc4 {
        let e = dst.ohpc4.entry(year).or_default();
        for (i, v) in c.iter().enumerate() {
            e[i] += v;
        }
    }
    for (year, c) in src.types {
        let e = dst.types.entry(year).or_default();
        for (i, v) in c.iter().enumerate() {
            e[i] += v;
        }
    }

    for (name, per_year) in src.libdnf {
        let e = dst.libdnf.entry(name).or_default();
        for (year, count) in per_year {
            *e.entry(year).or_insert(0) += count;
        }
    }
}

// Bump this whenever a change to the parsing alters the counts a given log
// file produces. A cache entry written by a different version is ignored and
// the file is parsed again, so forgetting to bump it is the one way to get
// stale numbers out of this program.
const CACHE_VERSION: u32 = 1;
const CACHE_MAGIC: &[u8; 8] = b"OHPCLOG\0";

struct Enc(Vec<u8>);

impl Enc {
    fn u8(&mut self, v: u8) {
        self.0.push(v);
    }
    fn u32(&mut self, v: u32) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    fn u64(&mut self, v: u64) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    fn i64(&mut self, v: i64) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    fn u128(&mut self, v: u128) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    fn bytes(&mut self, b: &[u8]) {
        self.u64(b.len() as u64);
        self.0.extend_from_slice(b);
    }
}

struct Dec<'a> {
    b: &'a [u8],
    pos: usize,
}

// Every read is checked and returns None on a short or malformed entry. A
// cache file that cannot be decoded is treated as absent, never as an error
// and never as a panic, so a truncated or corrupt entry only costs a reparse.
impl<'a> Dec<'a> {
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.pos.checked_add(n)?;
        let out = self.b.get(self.pos..end)?;
        self.pos = end;
        Some(out)
    }
    fn u8(&mut self) -> Option<u8> {
        Some(self.take(1)?[0])
    }
    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_le_bytes(self.take(4)?.try_into().ok()?))
    }
    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_le_bytes(self.take(8)?.try_into().ok()?))
    }
    fn i64(&mut self) -> Option<i64> {
        Some(i64::from_le_bytes(self.take(8)?.try_into().ok()?))
    }
    fn u128(&mut self) -> Option<u128> {
        Some(u128::from_le_bytes(self.take(16)?.try_into().ok()?))
    }
    // A corrupt length must not make the program reserve gigabytes, so a
    // count is only accepted when at least that many bytes are still left.
    fn count(&mut self) -> Option<usize> {
        let n = usize::try_from(self.u64()?).ok()?;
        match n <= self.b.len().saturating_sub(self.pos) {
            true => Some(n),
            false => None,
        }
    }
    fn bytes(&mut self) -> Option<&'a [u8]> {
        let n = self.count()?;
        self.take(n)
    }
}

fn encode_counts(counts: &ChunkCounts) -> Vec<u8> {
    let mut e = Enc(Vec::with_capacity(1 << 20));
    e.u64(counts.lines as u64);
    for v in counts.release_lines {
        e.u64(v as u64);
    }

    e.u64(counts.years.len() as u64);
    for (year, y) in &counts.years {
        e.i64(*year);
        e.i64(y.overall);
        e.u64(y.size);
        for v in y.ohpc {
            e.i64(v);
        }
        e.u64(y.ipv4.len() as u64);
        for (ip, releases) in &y.ipv4 {
            e.u32(*ip);
            e.u8(*releases);
        }
        e.u64(y.ipv6.len() as u64);
        for (ip, releases) in &y.ipv6 {
            e.u128(*ip);
            e.u8(*releases);
        }
    }

    e.u64(counts.months.len() as u64);
    for ((year, month), m) in &counts.months {
        e.i64(*year);
        e.i64(*month);
        e.i64(m.overall);
        e.u64(m.size);
        for v in m.ohpc {
            e.i64(v);
        }
        e.u64(m.ipv4.len() as u64);
        for (ip, stats) in &m.ipv4 {
            e.u32(*ip);
            e.i64(stats.requests);
            e.u8(stats.releases);
        }
        e.u64(m.ipv6.len() as u64);
        for (ip, stats) in &m.ipv6 {
            e.u128(*ip);
            e.i64(stats.requests);
            e.u8(stats.releases);
        }
    }

    for map in [&counts.ohpc1, &counts.ohpc2, &counts.ohpc4] {
        e.u64(map.len() as u64);
        for (year, c) in map.iter() {
            e.i64(*year);
            for v in c {
                e.i64(*v);
            }
        }
    }
    for map in [&counts.ohpc3, &counts.types] {
        e.u64(map.len() as u64);
        for (year, c) in map.iter() {
            e.i64(*year);
            for v in c {
                e.i64(*v);
            }
        }
    }

    e.u64(counts.libdnf.len() as u64);
    for (name, per_year) in &counts.libdnf {
        e.bytes(name.as_bytes());
        e.u64(per_year.len() as u64);
        for (year, count) in per_year {
            e.i64(*year);
            e.i64(*count);
        }
    }

    e.0
}

fn decode_counts(d: &mut Dec) -> Option<ChunkCounts> {
    let mut counts = ChunkCounts {
        lines: usize::try_from(d.u64()?).ok()?,
        ..Default::default()
    };
    for slot in counts.release_lines.iter_mut() {
        *slot = usize::try_from(d.u64()?).ok()?;
    }

    for _ in 0..d.count()? {
        let year = d.i64()?;
        let mut y = YearCounts {
            overall: d.i64()?,
            size: d.u64()?,
            ..Default::default()
        };
        for slot in y.ohpc.iter_mut() {
            *slot = d.i64()?;
        }
        for _ in 0..d.count()? {
            y.ipv4.insert(d.u32()?, d.u8()?);
        }
        for _ in 0..d.count()? {
            y.ipv6.insert(d.u128()?, d.u8()?);
        }
        counts.years.insert(year, y);
    }

    for _ in 0..d.count()? {
        let year = d.i64()?;
        let month = d.i64()?;
        let mut m = MonthCounts {
            overall: d.i64()?,
            size: d.u64()?,
            ..Default::default()
        };
        for slot in m.ohpc.iter_mut() {
            *slot = d.i64()?;
        }
        for _ in 0..d.count()? {
            let ip = d.u32()?;
            m.ipv4.insert(ip, IpStats {
                requests: d.i64()?,
                releases: d.u8()?,
            });
        }
        for _ in 0..d.count()? {
            let ip = d.u128()?;
            m.ipv6.insert(ip, IpStats {
                requests: d.i64()?,
                releases: d.u8()?,
            });
        }
        counts.months.insert((year, month), m);
    }

    for map in [&mut counts.ohpc1, &mut counts.ohpc2, &mut counts.ohpc4] {
        for _ in 0..d.count()? {
            let year = d.i64()?;
            map.insert(year, [d.i64()?, d.i64()?]);
        }
    }
    for map in [&mut counts.ohpc3, &mut counts.types] {
        for _ in 0..d.count()? {
            let year = d.i64()?;
            map.insert(year, [d.i64()?, d.i64()?, d.i64()?]);
        }
    }

    for _ in 0..d.count()? {
        let name = String::from_utf8(d.bytes()?.to_vec()).ok()?;
        let mut per_year = FxHashMap::default();
        for _ in 0..d.count()? {
            per_year.insert(d.i64()?, d.i64()?);
        }
        counts.libdnf.insert(name, per_year);
    }

    Some(counts)
}

// Identity of an input file. A cache entry is only used when all of these
// still match, so a log that was rotated, appended to or replaced is parsed
// again. The daily log still grows, so it is reparsed every run; the files
// that make up the bulk of the input no longer are.
struct FileId {
    len: u64,
    mtime_secs: i64,
    mtime_nanos: u32,
    inode: u64,
}

fn file_id(meta: &std::fs::Metadata) -> FileId {
    use std::os::unix::fs::MetadataExt;
    FileId {
        len: meta.len(),
        mtime_secs: meta.mtime(),
        mtime_nanos: meta.mtime_nsec() as u32,
        inode: meta.ino(),
    }
}

// The cache file is named after a hash of the input path so that logs from
// different directories cannot collide, and the path is stored in the entry
// as well so a hash collision is caught rather than silently believed.
fn cache_path(cache_dir: &Path, input: &str) -> PathBuf {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for b in input.as_bytes() {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    cache_dir.join(format!("{hash:016x}.counts"))
}

fn read_cache(cache_dir: &Path, input: &str, id: &FileId) -> Option<ChunkCounts> {
    let raw = fs::read(cache_path(cache_dir, input)).ok()?;
    let mut d = Dec { b: &raw, pos: 0 };

    if d.take(CACHE_MAGIC.len())? != CACHE_MAGIC {
        return None;
    }
    if d.u32()? != CACHE_VERSION {
        return None;
    }
    if d.bytes()? != input.as_bytes() {
        return None;
    }
    if d.u64()? != id.len
        || d.i64()? != id.mtime_secs
        || d.u32()? != id.mtime_nanos
        || d.u64()? != id.inode
    {
        return None;
    }

    decode_counts(&mut d)
}

fn write_cache(
    cache_dir: &Path,
    input: &str,
    id: &FileId,
    counts: &ChunkCounts,
) -> Result<(), Box<dyn std::error::Error>> {
    fs::create_dir_all(cache_dir)?;

    let mut e = Enc(Vec::new());
    e.0.extend_from_slice(CACHE_MAGIC);
    e.u32(CACHE_VERSION);
    e.bytes(input.as_bytes());
    e.u64(id.len);
    e.i64(id.mtime_secs);
    e.u32(id.mtime_nanos);
    e.u64(id.inode);
    e.0.extend_from_slice(&encode_counts(counts));

    write_private(&cache_path(cache_dir, input), &e.0)
}

/// The input path an entry was written for, or `None` when the entry is not
/// one this binary can use.
///
/// Only the header is read. An entry runs to megabytes, so decoding every
/// address set just to find out which file an entry belongs to would cost
/// far more than the prune saves.
fn read_cache_header(path: &Path) -> Option<String> {
    // PATH_MAX is 4096 on Linux, so a buffer twice that always holds the
    // magic, the version and the whole path.
    let mut head = [0_u8; 8192];
    let mut file = File::open(path).ok()?;
    let read = file.read(&mut head).ok()?;
    let mut d = Dec {
        b: &head[..read],
        pos: 0,
    };

    if d.take(CACHE_MAGIC.len())? != CACHE_MAGIC || d.u32()? != CACHE_VERSION {
        return None;
    }

    String::from_utf8(d.bytes()?.to_vec()).ok()
}

/// Deletes the entries the cache has no further use for.
///
/// Nothing else removes them, so without this a daily log that has been
/// folded into its monthly file and deleted leaves its entry behind for
/// good, and the directory grows without limit.
fn prune_cache(cache_dir: &Path) {
    let entries = match fs::read_dir(cache_dir) {
        Ok(entries) => entries,
        Err(e) => {
            eprintln!(
                "Reading the cache directory '{}' failed: {e}",
                cache_dir.display()
            );
            return;
        }
    };

    let mut removed = 0;
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("counts") {
            continue;
        }

        let stale = match read_cache_header(&path) {
            // Corrupt, truncated, or written by another CACHE_VERSION: no
            // run can use it again, so it is only taking up space.
            None => true,
            // A relative path was resolved against a working directory this
            // run need not share, so testing it here could delete an entry
            // that is in fact still in use. Only an absolute path is checked.
            Some(input) => {
                let input = Path::new(&input);
                input.is_absolute() && !input.exists()
            }
        };

        if stale {
            match fs::remove_file(&path) {
                Ok(()) => removed += 1,
                Err(e) => eprintln!("Removing '{}' failed: {e}", path.display()),
            }
        }
    }

    if removed > 0 {
        print_step(format!("Removed {removed} unused cache entries"));
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

    let cache_dir = params.cache_directory.as_ref().map(PathBuf::from);

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
        // A rotated log never changes again, so its counters are taken from
        // the cache and the file itself is not read at all.
        let id = access_log.metadata().as_ref().map(file_id).ok();
        if let (Some(dir), Some(id)) = (&cache_dir, &id) {
            if let Some(counts) = read_cache(dir, &input, id) {
                print_step(format!("Using the cached counts for '{input}'"));
                merge_chunk(counts);
                continue;
            }
        }

        print_step(format!("Using '{input:}' as input"));

        // With a cache the chunks are folded into one ChunkCounts per file so
        // that it can be stored; without one they go straight into the
        // globals as before, which keeps the default memory profile the same.
        let file_counts = cache_dir
            .as_ref()
            .and(id.as_ref())
            .map(|_| Mutex::new(ChunkCounts::default()));
        let accumulator = file_counts.as_ref();

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
                    match accumulator {
                        Some(acc) => merge_counts(&mut acc.lock().unwrap(), counts),
                        _ => merge_chunk(counts),
                    }
                    CHUNKS_IN_FLIGHT.fetch_sub(1, Ordering::Release);
                });
                s = next_s;
            }
            pb.finish();
        });

        if let (Some(dir), Some(id), Some(acc)) = (&cache_dir, &id, file_counts) {
            let counts = acc.into_inner().unwrap_or_default();
            // A cache that cannot be written only costs the next run some
            // time, so it must not stop this one from publishing its page.
            if let Err(e) = write_cache(dir, &input, id, &counts) {
                eprintln!("Writing the cache entry for '{input}' failed: {e}");
            }
            merge_chunk(counts);
        }
    }

    if let Some(dir) = &cache_dir {
        prune_cache(dir);
    }

    if let Err(e) = create_plots(params, start) {
        println!("Error creating diagrams: {e}");
        process::exit(1);
    }
}

/// Colours for the chart series, matching the order the traces are added.
const SERIES_COLORS: [RGBColor; 5] = [
    RGBColor(31, 119, 180),
    RGBColor(255, 127, 14),
    RGBColor(44, 160, 44),
    RGBColor(214, 39, 40),
    RGBColor(148, 103, 189),
];

fn counts_to_f64(values: &[i64]) -> Vec<f64> {
    values.iter().map(|v| *v as f64).collect()
}

fn sizes_to_f64(values: &[u64]) -> Vec<f64> {
    values.iter().map(|v| *v as f64).collect()
}

fn format_count(v: f64) -> String {
    format!("{}", v.round() as i64)
}

fn format_bytes(v: f64) -> String {
    const UNITS: [&str; 6] = ["B", "kB", "MB", "GB", "TB", "PB"];
    let mut v = v;
    let mut unit = 0;
    while v >= 1000.0 && unit < UNITS.len() - 1 {
        v /= 1000.0;
        unit += 1;
    }
    format!("{:.1} {}", v, UNITS[unit])
}

/// Write a line chart as a standalone SVG.
///
/// This replaces plotly's Kaleido export, which needed an external renderer
/// binary on the machine and was deprecated upstream. plotters renders SVG in
/// pure Rust, so nothing outside the binary is required.
// Publishing a file by writing straight into it exposes every intermediate
// state to anyone fetching it, and leaves a truncated file behind if the run
// dies partway through. Write to a temporary file beside the destination and
// rename it into place instead: rename is atomic within a filesystem, so a
// reader sees either the previous file or the complete new one, and a failed
// run leaves the previous file untouched.
fn write_atomic(path: &Path, contents: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    write_file(path, contents, None)
}

/// Writes `contents` at `path` as [`write_atomic`] does, but readable only by
/// the user running the analyzer.
///
/// A cache entry holds the raw client addresses the unique visitor counts are
/// derived from. The default permissions of a new file make those readable by
/// anything that can reach the directory, which publishes them outright when
/// the cache was pointed at a directory a web server serves.
fn write_private(path: &Path, contents: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    write_file(path, contents, Some(0o600))
}

fn write_file(
    path: &Path,
    contents: &[u8],
    mode: Option<u32>,
) -> Result<(), Box<dyn std::error::Error>> {
    // The process id keeps two runs writing the same destination from
    // interleaving into one temporary file and renaming the result into
    // place, which would publish exactly the corrupt file this avoids.
    let mut tmp = path.as_os_str().to_os_string();
    tmp.push(format!(".{}.tmp", process::id()));
    let tmp = PathBuf::from(tmp);

    let result = (|| -> std::io::Result<()> {
        let mut options = fs::OpenOptions::new();
        options.write(true).create(true).truncate(true);
        if let Some(mode) = mode {
            options.mode(mode);
        }
        let mut file = options.open(&tmp)?;
        if let Some(mode) = mode {
            // The mode above applies only when the open creates the file, so
            // a temporary left behind by a crashed run of the same pid would
            // keep whatever permissions it already had. Setting them again
            // covers that, before anything is written into the file.
            file.set_permissions(fs::Permissions::from_mode(mode))?;
        }
        file.write_all(contents)?;
        // Rename only orders the directory entry against the file contents if
        // those contents have reached the disk, so flush before renaming.
        file.sync_all()?;
        fs::rename(&tmp, path)
    })();

    if result.is_err() {
        // Do not leave the partial file lying next to the published one.
        let _ = fs::remove_file(&tmp);
    }

    Ok(result?)
}

/// How a chart is drawn. The HTML page uses a stacked bar chart for the
/// overall accesses and lines everywhere else.
#[derive(Clone, Copy, PartialEq)]
enum ChartStyle {
    Lines,
    StackedBars,
}

/// A legend naming every series stops being readable long before the couple
/// of hundred countries the country chart has, so above this many series
/// only the largest ones are named. The others are still drawn.
const MAX_LEGEND_ENTRIES: usize = 12;

/// Colour for series `i`. The first five are the fixed palette the charts
/// have always used, so the charts that had an SVG before keep their
/// colours; beyond that the hue is rotated by the golden angle and the
/// lightness cycled, which keeps dozens of series apart reasonably well.
fn series_color(i: usize) -> RGBColor {
    if let Some(color) = SERIES_COLORS.get(i) {
        return *color;
    }
    let n = (i - SERIES_COLORS.len()) as f64;
    let hue = (n * 137.507_764_05) % 360.0;
    let lightness = match i % 3 {
        0 => 0.35,
        1 => 0.50,
        _ => 0.65,
    };
    hsl_to_rgb(hue, 0.65, lightness)
}

fn hsl_to_rgb(h: f64, s: f64, l: f64) -> RGBColor {
    let c = (1.0 - (2.0 * l - 1.0).abs()) * s;
    let x = c * (1.0 - ((h / 60.0) % 2.0 - 1.0).abs());
    let m = l - c / 2.0;
    let (r, g, b) = match (h / 60.0) as u32 {
        0 => (c, x, 0.0),
        1 => (x, c, 0.0),
        2 => (0.0, c, x),
        3 => (0.0, x, c),
        4 => (x, 0.0, c),
        _ => (c, 0.0, x),
    };
    RGBColor(
        ((r + m) * 255.0).round() as u8,
        ((g + m) * 255.0).round() as u8,
        ((b + m) * 255.0).round() as u8,
    )
}

/// One chart series: a name and the points it has, each tagged with the
/// label of the position on the x axis it belongs to. The series of a chart
/// do not all cover the same range -- a release that only appeared in 2024
/// has no earlier points, and most countries are missing from most months --
/// so every point carries its own label rather than relying on position.
type SvgSeries = (String, Vec<(String, f64)>);

// A chart that cannot be rendered must not take the HTML page down with it.
// Before this the error propagated out of create_plots, which had already
// truncated the published index.html, so one failing SVG left the site
// showing nothing but the charts written before it.
fn write_svg_chart(
    path: &Path,
    title: &str,
    series: &[SvgSeries],
    y_format: fn(f64) -> String,
    style: ChartStyle,
) {
    if let Err(e) = render_svg_chart(path, title, series, y_format, style) {
        eprintln!("Rendering {} failed: {e}", path.display());
    }
}

fn render_svg_chart(
    path: &Path,
    title: &str,
    series: &[SvgSeries],
    y_format: fn(f64) -> String,
    style: ChartStyle,
) -> Result<(), Box<dyn std::error::Error>> {
    // The x axis is the sorted union of every label any series mentions.
    // Years and zero padded year-months both sort into the right order as
    // strings, which is the order the HTML charts use as well.
    let mut x_labels: Vec<&str> = series
        .iter()
        .flat_map(|(_, points)| points.iter().map(|(label, _)| label.as_str()))
        .collect();
    x_labels.sort_unstable();
    x_labels.dedup();

    if x_labels.is_empty() {
        return Ok(());
    }

    let index: FxHashMap<&str, usize> = x_labels
        .iter()
        .enumerate()
        .map(|(i, label)| (*label, i))
        .collect();

    // A stacked chart is as tall as the whole stack, not as its largest
    // single contribution.
    let y_max = match style {
        ChartStyle::Lines => series
            .iter()
            .flat_map(|(_, points)| points.iter().map(|(_, v)| *v))
            .fold(0.0_f64, f64::max),
        ChartStyle::StackedBars => {
            let mut totals = vec![0.0_f64; x_labels.len()];
            for (_, points) in series {
                for (label, v) in points {
                    if let Some(i) = index.get(label.as_str()) {
                        totals[*i] += *v;
                    }
                }
            }
            totals.iter().copied().fold(0.0_f64, f64::max)
        }
    };
    // Leave a little headroom so the highest point is not on the frame.
    let y_max = if y_max > 0.0 { y_max * 1.05 } else { 1.0 };

    // Name every series while they still fit, otherwise only the tallest
    // ones. Picking a subset never reorders the legend, because the entries
    // are drawn in the order the series are added either way.
    let mut labelled = vec![series.len() <= MAX_LEGEND_ENTRIES; series.len()];
    if series.len() > MAX_LEGEND_ENTRIES {
        let peak = |s: &SvgSeries| {
            s.1.iter()
                .map(|(_, v)| *v)
                .fold(f64::NEG_INFINITY, f64::max)
        };
        let mut order: Vec<usize> = (0..series.len()).collect();
        // The index breaks ties so the choice does not depend on the sort.
        order.sort_by(|a, b| {
            peak(&series[*b])
                .partial_cmp(&peak(&series[*a]))
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.cmp(b))
        });
        for i in order.into_iter().take(MAX_LEGEND_ENTRIES) {
            labelled[i] = true;
        }
    }

    let mut buf = String::new();
    {
        let root = SVGBackend::with_string(&mut buf, (1600, 600)).into_drawing_area();
        root.fill(&WHITE)?;

        let caption = (title, ("sans-serif", 28));
        let label_text = |v: f64| -> String {
            let nearest = v.round();
            match (v - nearest).abs() > 1e-6 || nearest < 0.0 {
                true => String::new(),
                false => x_labels
                    .get(nearest as usize)
                    .map(|l| (*l).to_string())
                    .unwrap_or_default(),
            }
        };

        match style {
            ChartStyle::Lines => {
                let x_max = x_labels.len().saturating_sub(1).max(1) as i32;
                let mut chart = ChartBuilder::on(&root)
                    .caption(caption.0, caption.1)
                    .margin(20)
                    .x_label_area_size(60)
                    .y_label_area_size(110)
                    .build_cartesian_2d(0_i32..x_max, 0.0_f64..y_max)?;

                chart
                    .configure_mesh()
                    .x_labels(x_labels.len().min(12))
                    .x_label_formatter(&|v| label_text(*v as f64))
                    .y_label_formatter(&|v| y_format(*v))
                    .draw()?;

                for (i, (name, points)) in series.iter().enumerate() {
                    let color = series_color(i);
                    let mut line: Vec<(i32, f64)> = points
                        .iter()
                        .filter_map(|(label, v)| Some((*index.get(label.as_str())? as i32, *v)))
                        .collect();
                    line.sort_by_key(|(x, _)| *x);

                    let drawn = chart.draw_series(LineSeries::new(line, color.stroke_width(2)))?;
                    if labelled[i] {
                        drawn.label(name.as_str()).legend(move |(x, y)| {
                            PathElement::new(vec![(x, y), (x + 20, y)], color.stroke_width(2))
                        });
                    }
                }

                chart
                    .configure_series_labels()
                    .background_style(WHITE.mix(0.85))
                    .border_style(BLACK)
                    .draw()?;
            }
            ChartStyle::StackedBars => {
                let mut chart = ChartBuilder::on(&root)
                    .caption(caption.0, caption.1)
                    .margin(20)
                    .x_label_area_size(60)
                    .y_label_area_size(110)
                    .build_cartesian_2d(-0.5_f64..(x_labels.len() as f64 - 0.5), 0.0_f64..y_max)?;

                chart
                    .configure_mesh()
                    .x_labels(x_labels.len().min(12))
                    .x_label_formatter(&|v| label_text(*v))
                    .y_label_formatter(&|v| y_format(*v))
                    .draw()?;

                // Each series sits on top of the ones already drawn, which
                // is what BarMode::Stack does in the HTML chart.
                let mut bottoms = vec![0.0_f64; x_labels.len()];
                for (i, (name, points)) in series.iter().enumerate() {
                    let color = series_color(i);
                    let mut bars = Vec::new();
                    for (label, v) in points {
                        let Some(slot) = index.get(label.as_str()) else {
                            continue;
                        };
                        let bottom = bottoms[*slot];
                        let top = bottom + *v;
                        bottoms[*slot] = top;
                        let x = *slot as f64;
                        bars.push(Rectangle::new(
                            [(x - 0.3, bottom), (x + 0.3, top)],
                            color.filled(),
                        ));
                    }

                    let drawn = chart.draw_series(bars)?;
                    if labelled[i] {
                        drawn.label(name.as_str()).legend(move |(x, y)| {
                            Rectangle::new([(x, y - 5), (x + 20, y + 5)], color.filled())
                        });
                    }
                }

                chart
                    .configure_series_labels()
                    .background_style(WHITE.mix(0.85))
                    .border_style(BLACK)
                    .draw()?;
            }
        }

        root.present()?;
    }

    write_atomic(path, buf.as_bytes())
}

/// Build one series that has its own x range, from the labels it covers
/// and the values belonging to them.
fn ragged_series(name: &str, labels: &[String], values: &[i64]) -> SvgSeries {
    (
        name.to_string(),
        labels
            .iter()
            .cloned()
            .zip(values.iter().map(|v| *v as f64))
            .collect(),
    )
}

/// The labels of a chart whose x axis is a list of years.
fn year_labels(years: &[i64]) -> Vec<String> {
    years.iter().map(|y| y.to_string()).collect()
}

/// Build the series for a chart whose series all cover the same labels.
fn aligned_series(labels: &[String], series: &[(&str, Vec<f64>)]) -> Vec<SvgSeries> {
    series
        .iter()
        .map(|(name, values)| {
            (
                (*name).to_string(),
                labels
                    .iter()
                    .cloned()
                    .zip(values.iter().copied())
                    .collect::<Vec<_>>(),
            )
        })
        .collect()
}

fn create_overall_plot(params: &Args) -> String {
    let labels = vec!["Accesses"];
    let ohpc_1 = OHPC_1.load(Ordering::SeqCst);
    let ohpc_2 = OHPC_2.load(Ordering::SeqCst);
    let ohpc_3 = OHPC_3.load(Ordering::SeqCst);
    let ohpc_4 = OHPC_4.load(Ordering::SeqCst);
    let other = OVERALL
        .load(Ordering::SeqCst)
        .saturating_sub(ohpc_1 + ohpc_2 + ohpc_3 + ohpc_4);

    if !params.no_svg {
        // The HTML chart stacks these five bars on one category, so the SVG
        // does the same rather than putting them side by side.
        let accesses = |name: &str, value: usize| -> SvgSeries {
            (name.to_string(), vec![("Accesses".to_string(), value as f64)])
        };
        write_svg_chart(
            &Path::new(&params.output_directory).join("overall_accesses.svg"),
            "OHPC overall repository accesses",
            &[
                accesses("Release 1.3.x", ohpc_1),
                accesses("Release 2.x", ohpc_2),
                accesses("Release 3.x", ohpc_3),
                accesses("Release 4.x", ohpc_4),
                accesses("Other", other),
            ],
            format_count,
            ChartStyle::StackedBars,
        );
    }

    let mut plot = Plot::new();
    plot.add_trace(Bar::new(labels.clone(), vec![ohpc_1]).name("Release 1.3.x"));
    plot.add_trace(Bar::new(labels.clone(), vec![ohpc_2]).name("Release 2.x"));
    plot.add_trace(Bar::new(labels.clone(), vec![ohpc_3]).name("Release 3.x"));
    plot.add_trace(Bar::new(labels.clone(), vec![ohpc_4]).name("Release 4.x"));
    plot.add_trace(Bar::new(labels, vec![other]).name("Other"));
    plot.set_layout(
        Layout::new()
            .bar_mode(BarMode::Stack)
            .title("OHPC overall repository accesses"),
    );

    plot.to_inline_html(None)
}

fn create_type_plot(params: &Args) -> String {
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

    if !params.no_svg {
        write_svg_chart(
            &Path::new(&params.output_directory).join("file_types_per_year.svg"),
            "OHPC file types per year",
            &aligned_series(
                &year_labels(&years),
                &[
                    ("TAR", counts_to_f64(&tar)),
                    ("RPM", counts_to_f64(&rpm)),
                    ("repomd.xml", counts_to_f64(&repomd_xml)),
                ],
            ),
            format_count,
            ChartStyle::Lines,
        );
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
    params: &Args,
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

    if !params.no_svg {
        write_svg_chart(
            &Path::new(&params.output_directory).join("requests_per_year.svg"),
            "OHPC repository requests per year",
            &aligned_series(
                &year_labels(years),
                &[
                    ("OHPC 1.3.x", counts_to_f64(&ohpc_1)),
                    ("OHPC 2.x", counts_to_f64(&ohpc_2)),
                    ("OHPC 3.x", counts_to_f64(&ohpc_3)),
                    ("OHPC 4.x", counts_to_f64(&ohpc_4)),
                    ("Total", counts_to_f64(&overall)),
                ],
            ),
            format_count,
            ChartStyle::Lines,
        );
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

    if !params.no_svg {
        write_svg_chart(
            &Path::new(&params.output_directory).join("unique_visitors_per_year.svg"),
            "Unique OHPC repository requests per year",
            &aligned_series(
                &years.iter().map(|y| y.to_string()).collect::<Vec<_>>(),
                &[
                    ("OHPC 1.3.x", counts_to_f64(&unique_ohpc_1)),
                    ("OHPC 2.x", counts_to_f64(&unique_ohpc_2)),
                    ("OHPC 3.x", counts_to_f64(&unique_ohpc_3)),
                    ("OHPC 4.x", counts_to_f64(&unique_ohpc_4)),
                    ("Total", counts_to_f64(&unique_overall)),
                ],
            ),
            format_count,
            ChartStyle::Lines,
        );
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

    if !params.no_svg {
        write_svg_chart(
            &Path::new(&params.output_directory).join("size_per_year.svg"),
            "OHPC data downloaded per year",
            &aligned_series(
                &years.iter().map(|y| y.to_string()).collect::<Vec<_>>(),
                &[("Total", sizes_to_f64(&size))],
            ),
            format_bytes,
            ChartStyle::Lines,
        );
    }

    let mut plot_size_per_year = Plot::new();
    let size_per_year = Scatter::new(years.clone(), size).name("Total");
    plot_size_per_year.add_trace(size_per_year);
    let layout_size_per_year = Layout::new()
        .title("OHPC data downloaded per year")
        .x_axis(Axis::new().tick_values(ticks.clone()));
    plot_size_per_year.set_layout(layout_size_per_year);

    Ok(plot_size_per_year.to_inline_html(None))
}

fn create_repository_requests_per_month(
    params: &Args,
) -> Result<String, Box<dyn std::error::Error>> {
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

    if !params.no_svg {
        write_svg_chart(
            &Path::new(&params.output_directory).join("requests_per_month.svg"),
            "OHPC repository requests per month",
            &aligned_series(
                &year_months,
                &[
                    ("OHPC 1.3.x", counts_to_f64(&ohpc_1_per_month)),
                    ("OHPC 2.x", counts_to_f64(&ohpc_2_per_month)),
                    ("OHPC 3.x", counts_to_f64(&ohpc_3_per_month)),
                    ("OHPC 4.x", counts_to_f64(&ohpc_4_per_month)),
                    ("Total", counts_to_f64(&overall_per_month)),
                ],
            ),
            format_count,
            ChartStyle::Lines,
        );
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

    if !params.no_svg {
        write_svg_chart(
            &Path::new(&params.output_directory).join("unique_visitors_per_month.svg"),
            "Unique OHPC repository requests per month",
            &aligned_series(
                &year_months,
                &[
                    ("OHPC 1.3.x", counts_to_f64(&unique_ohpc_1_per_month)),
                    ("OHPC 2.x", counts_to_f64(&unique_ohpc_2_per_month)),
                    ("OHPC 3.x", counts_to_f64(&unique_ohpc_3_per_month)),
                    ("OHPC 4.x", counts_to_f64(&unique_ohpc_4_per_month)),
                    ("Total", counts_to_f64(&unique_overall_per_month)),
                ],
            ),
            format_count,
            ChartStyle::Lines,
        );
    }

    let mut plot = Plot::new();
    plot.add_trace(Scatter::new(year_months.clone(), unique_ohpc_1_per_month).name("OHPC 1.3.x"));
    plot.add_trace(Scatter::new(year_months.clone(), unique_ohpc_2_per_month).name("OHPC 2.x"));
    plot.add_trace(Scatter::new(year_months.clone(), unique_ohpc_3_per_month).name("OHPC 3.x"));
    plot.add_trace(Scatter::new(year_months.clone(), unique_ohpc_4_per_month).name("OHPC 4.x"));
    plot.add_trace(Scatter::new(year_months.clone(), unique_overall_per_month).name("Total"));
    plot.set_layout(Layout::new().title("Unique OHPC repository requests per month"));

    Ok(plot.to_inline_html(None))
}

fn create_repository_requests_per_year_and_distribution(
    params: &Args,
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
    // Each release covers its own span of years, so the series are built
    // with the labels they actually have rather than a shared axis.
    let mut svg: Vec<SvgSeries> = Vec::new();
    svg.push(ragged_series(
        "OHPC SLES 1.3.x",
        &year_labels(&years),
        &ohpc_1_sles,
    ));
    svg.push(ragged_series(
        "OHPC RHEL 1.3.x",
        &year_labels(&years),
        &ohpc_1_rhel,
    ));

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
    svg.push(ragged_series(
        "OHPC SLES 2.x",
        &year_labels(&years),
        &ohpc_2_sles,
    ));
    svg.push(ragged_series(
        "OHPC RHEL 2.x",
        &year_labels(&years),
        &ohpc_2_rhel,
    ));
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
    svg.push(ragged_series(
        "OHPC SLES 3.x",
        &year_labels(&years),
        &ohpc_3_sles,
    ));
    svg.push(ragged_series(
        "OHPC RHEL 3.x",
        &year_labels(&years),
        &ohpc_3_rhel,
    ));
    svg.push(ragged_series(
        "OHPC openEuler 3.x",
        &year_labels(&years),
        &ohpc_3_openeuler,
    ));
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
    svg.push(ragged_series(
        "OHPC RHEL 4.x",
        &year_labels(&years),
        &ohpc_4_rhel,
    ));
    svg.push(ragged_series(
        "OHPC openEuler 4.x",
        &year_labels(&years),
        &ohpc_4_openeuler,
    ));
    plot.add_trace(Scatter::new(years.clone(), ohpc_4_rhel).name("OHPC RHEL 4.x"));
    plot.add_trace(Scatter::new(years.clone(), ohpc_4_openeuler).name("OHPC openEuler 4.x"));

    if !params.no_svg {
        write_svg_chart(
            &Path::new(&params.output_directory)
                .join("requests_per_year_and_distribution.svg"),
            "OHPC repository requests per year and distribution",
            &svg,
            format_count,
            ChartStyle::Lines,
        );
    }

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

    if !params.no_svg {
        write_svg_chart(
            &Path::new(&params.output_directory).join("size_per_month.svg"),
            "OHPC data downloaded per month",
            &aligned_series(&year_months, &[("Total", sizes_to_f64(&size_per_month))]),
            format_bytes,
            ChartStyle::Lines,
        );
    }

    let mut plot = Plot::new();
    plot.add_trace(Scatter::new(year_months.clone(), size_per_month).name("Total"));
    plot.set_layout(Layout::new().title("OHPC data downloaded per month"));

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
    let mut svg: Vec<SvgSeries> = Vec::new();
    for result in names {
        svg.push(ragged_series(
            result,
            &country_results[result].year_months,
            &country_results[result].count,
        ));
        let trace_countries = Scatter::new(
            country_results[result].year_months.clone(),
            country_results[result].count.clone(),
        )
        .name(result);
        plot.add_trace(trace_countries);
    }

    if !params.no_svg {
        write_svg_chart(
            &Path::new(&params.output_directory).join("country_per_month.svg"),
            "OHPC repository country requests per month",
            &svg,
            format_count,
            ChartStyle::Lines,
        );
    }

    Ok(plot.to_inline_html(None))
}

fn create_libdnf_requests_per_year_and_distribution(
    json: &mut Json,
    params: &Args,
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

    // Iterating the map directly would emit the traces in hash order, so the
    // legend and the colour of every distribution changed from run to run.
    let mut names: Vec<&String> = libdnf_results.keys().collect();
    names.sort_unstable();
    let mut svg: Vec<SvgSeries> = Vec::new();
    for result in names {
        svg.push(ragged_series(
            result,
            &year_labels(&libdnf_results[result].years),
            &libdnf_results[result].count,
        ));
        let trace_libdnf = Scatter::new(
            libdnf_results[result].years.clone(),
            libdnf_results[result].count.clone(),
        )
        .name(result);
        plot_libdnf.add_trace(trace_libdnf);
    }

    if !params.no_svg {
        write_svg_chart(
            &Path::new(&params.output_directory)
                .join("libdnf_per_year_and_distribution.svg"),
            "OHPC repository libdnf requests per year and distribution",
            &svg,
            format_count,
            ChartStyle::Lines,
        );
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

    // The page is assembled in memory and published in one atomic rename at
    // the end. Writing it chart by chart into the live file meant the
    // published page was truncated for the whole of create_plots, and stayed
    // truncated if any chart failed.
    let mut page = String::with_capacity(512 * 1024);
    page.push_str(HTML_HEADER);
    page.push_str(&create_repository_requests_per_year(&years, &params)?);
    page.push_str(&create_unique_repository_requests_per_year(
        &years, &params, &mut json,
    )?);
    page.push_str(&create_repository_requests_per_month(&params)?);
    page.push_str(&create_unique_repository_requests_per_month(
        &params, &mut json,
    )?);
    page.push_str(&create_data_downloaded_per_year(&years, &params, &mut json)?);
    page.push_str(&create_data_downloaded_per_month(&params, &mut json)?);
    page.push_str(&create_repository_requests_per_year_and_distribution(&params)?);
    page.push_str(&create_libdnf_requests_per_year_and_distribution(
        &mut json, &params,
    )?);
    page.push_str(&create_overall_plot(&params));
    page.push_str(&create_type_plot(&params));
    page.push_str(&create_country_per_year_and_month(&params, &mut json)?);
    page.push_str("</div>");

    let duration = start.elapsed();
    let timestamp = chrono::Utc::now().to_rfc2822();
    page.push_str(&HTML_FOOTER.replace(
        "@@GENERATED@@",
        &format!("{timestamp}. Runtime: {duration:?}"),
    ));

    write_atomic(
        &Path::new(&params.output_directory).join(&params.html_output),
        page.as_bytes(),
    )?;
    write_atomic(
        &Path::new(&params.output_directory).join("stats.json"),
        &serde_json::to_vec(&json)?,
    )?;

    print_step(format!("Finished after {duration:?} at {timestamp}"));

    Ok(())
}
