extern crate indicatif;
extern crate regex;
extern crate serde;

use std::collections::hash_map::Entry;
use std::collections::{
    HashMap,
    HashSet,
};
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
    RwLock,
};
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
use rayon::prelude::*;
use regex::bytes::Regex;
use serde::Serialize;

static OVERALL: AtomicUsize = AtomicUsize::new(0);
static OHPC_1: AtomicUsize = AtomicUsize::new(0);
static OHPC_2: AtomicUsize = AtomicUsize::new(0);
static OHPC_3: AtomicUsize = AtomicUsize::new(0);
static STEPS: AtomicUsize = AtomicUsize::new(2);
static CALL_COUNT: AtomicUsize = AtomicUsize::new(1);
static CHUNK: AtomicUsize = AtomicUsize::new(0);

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

#[derive(Debug)]
struct ResultOverall {
    year: i64,
    ohpc_1: i64,
    ohpc_2: i64,
    ohpc_3: i64,
    overall: i64,
    unique_ohpc_1: i64,
    unique_ohpc_2: i64,
    unique_ohpc_3: i64,
    unique_overall: i64,
    size: u64,
    ipv4: HashSet<u32>,
    ipv6: HashSet<u128>,
}

#[derive(Debug)]
struct ResultOverallPerMonth {
    year: i64,
    month: i64,
    ohpc_1: i64,
    ohpc_2: i64,
    ohpc_3: i64,
    overall: i64,
    unique_ohpc_1: i64,
    unique_ohpc_2: i64,
    unique_ohpc_3: i64,
    unique_overall: i64,
    size: u64,
    ipv4: HashMap<u32, i64>,
    ipv6: HashMap<u128, i64>,
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
    overall: i64,
}
#[derive(Serialize)]
struct UniqueVisitorsPerMonth {
    year_month: String,
    ohpc1: i64,
    ohpc2: i64,
    ohpc3: i64,
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
static LIBDNF_RESULTS: RwLock<Vec<ResultLIBDNF>> = RwLock::new(Vec::new());
static COUNTRY_RESULTS: RwLock<Vec<ResultCountry>> = RwLock::new(Vec::new());
static TYPE_RESULTS: RwLock<Vec<ResultType>> = RwLock::new(Vec::new());

static HTML_HEADER: &str =
    "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\" /></head><body>   <div>
<script src=\"https://cdn.jsdelivr.net/npm/mathjax@3.2.2/es5/tex-svg.js\"></script>
<script src=\"https://cdn.plot.ly/plotly-2.12.1.min.js\"></script>";
static HTML_FOOTER: &str = "</div></body></html>";

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

fn count_type(elements: &[String], year: i64) {
    if elements.len() < 7 {
        return;
    }
    let rpm_found = elements[6].ends_with(".rpm");
    let tar_found = elements[6].ends_with(".tar");
    let repomd_xml_found = elements[6].ends_with("/repomd.xml");
    if !tar_found && !rpm_found && !repomd_xml_found {
        return;
    }
    let mut data = TYPE_RESULTS.write().unwrap();
    let mut entry_found = false;
    for result in data.as_mut_slice() {
        if result.year == year {
            entry_found = true;
            if rpm_found {
                result.rpm += 1;
            }
            if tar_found {
                result.tar += 1;
            }
            if repomd_xml_found {
                result.repomd_xml += 1;
            }
            break;
        }
    }

    if !entry_found {
        data.push(ResultType {
            year,
            tar: tar_found.into(),
            rpm: rpm_found.into(),
            repomd_xml: repomd_xml_found.into(),
        });
    }
}

fn count_libdnf(elements: &[String], year: i64) {
    if elements.len() < 12 {
        return;
    }
    if elements[11] != "\"libdnf" {
        return;
    }

    let user_agent_long = elements[12..].join(" ");
    let user_agent_vec: Vec<_> = user_agent_long.split(';').map(|s| s.to_string()).collect();
    let mut user_agent_short = user_agent_vec[0].clone();
    user_agent_short.truncate(user_agent_short.rfind(' ').unwrap());
    let mut user_agent = &user_agent_short[1..];
    if user_agent.is_empty() {
        return;
    }
    let change_name = HashMap::from([
        (
            "Red Hat Enterprise Linux Server",
            "Red Hat Enterprise Linux",
        ),
        ("CentOS AutoSD", "CentOS Linux"),
        ("CentOS release 8", "CentOS Linux"),
        ("RockyLinux", "Rocky Linux"),
    ]);
    if change_name.contains_key(&user_agent) {
        user_agent = change_name[&user_agent];
    }

    let mut data = LIBDNF_RESULTS.write().unwrap();
    for result in data.as_mut_slice() {
        if result.year == year && result.name == user_agent {
            result.count += 1;
            return;
        }
    }
    data.push(ResultLIBDNF {
        year,
        name: user_agent.to_string(),
        count: 1,
    });
}

fn update_distributions_ohpc_3(s: &[u8], year: i64) {
    let leap_15 = "Leap_15".as_bytes();
    let el_9 = "EL_9".as_bytes();
    let openeuler = "openEuler_22.03".as_bytes();
    let mut leap_15_found = false;
    let mut el_9_found = false;
    let mut openeuler_found = false;
    let search_leap = s
        .windows(leap_15.len())
        .position(|window| window == leap_15);
    if search_leap.is_some() {
        leap_15_found = true;
    } else {
        let search_centos = s.windows(el_9.len()).position(|window| window == el_9);
        if search_centos.is_some() {
            el_9_found = true;
        } else {
            let search_el = s
                .windows(openeuler.len())
                .position(|window| window == openeuler);
            if search_el.is_some() {
                openeuler_found = true;
            }
        }
    }
    if !el_9_found && !leap_15_found && !openeuler_found {
        return;
    }
    let mut data = OHPC3_RESULTS.write().unwrap();
    let mut year_found = false;
    for result in &*data {
        if result.year == year {
            year_found = true;
            break;
        }
    }
    if !year_found {
        data.push(ResultOHPC3 {
            year,
            sles: 0,
            rhel: 0,
            openeuler: 0,
        });
    }
    for result in data.as_mut_slice() {
        if result.year == year {
            if el_9_found {
                result.rhel += 1;
            }
            if leap_15_found {
                result.sles += 1;
            }
            if openeuler_found {
                result.openeuler += 1;
            }
            break;
        }
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

fn process_line(s: &[u8]) {
    let line = str::from_utf8(s).unwrap();
    let elements: Vec<_> = line.split(' ').map(|s| s.to_string()).collect();
    OVERALL.fetch_add(1, Ordering::SeqCst);
    if elements.len() < 3 {
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

    count_libdnf(&elements, year);
    count_type(&elements, year);
    let mut ohpc_1 = false;
    let mut ohpc_2 = false;
    let mut ohpc_3 = false;
    let substring_1 = "/ohpc-1.3/".as_bytes();
    let search_1 = s
        .windows(substring_1.len())
        .position(|window| window == substring_1);
    if let Some(search_result) = search_1 {
        OHPC_1.fetch_add(1, Ordering::SeqCst);
        ohpc_1 = true;
        let start = search_result + substring_1.len();
        let sle_12 = "SLE_12".as_bytes();
        let centos_7 = "CentOS_7".as_bytes();
        let mut centos_7_found = false;
        let mut sle_12_found = false;
        let search_centos = s[start..]
            .windows(centos_7.len())
            .position(|window| window == centos_7);
        if search_centos.is_some() {
            centos_7_found = true;
        } else {
            let search_sle = s[start..]
                .windows(sle_12.len())
                .position(|window| window == sle_12);
            if search_sle.is_some() {
                sle_12_found = true;
            }
        }

        let mut data = OHPC1_RESULTS.write().unwrap();
        let mut year_found = false;
        for result in &*data {
            if result.year == year {
                year_found = true;
                break;
            }
        }
        if !year_found {
            data.push(ResultOHPC1 {
                year,
                sles: 0,
                rhel: 0,
            });
        }
        for result in data.as_mut_slice() {
            if result.year == year {
                if centos_7_found {
                    result.rhel += 1;
                }
                if sle_12_found {
                    result.sles += 1;
                }
                break;
            }
        }
    }
    let substring_ohpc = "/OpenHPC/".as_bytes();
    let search_ohpc = s
        .windows(substring_ohpc.len())
        .position(|window| window == substring_ohpc);

    if let Some(search_result) = search_ohpc {
        let substring_2 = "2/".as_bytes();
        let start = search_result + substring_ohpc.len();
        if &s[start..start + 2] == substring_2 {
            let leap_15 = "Leap_15".as_bytes();
            let centos_8 = "CentOS_8".as_bytes();
            let el_8 = "EL_8".as_bytes();
            let mut leap_15_found = false;
            let mut el_8_found = false;
            OHPC_2.fetch_add(1, Ordering::SeqCst);
            ohpc_2 = true;
            let search_leap = s[start + 2..]
                .windows(leap_15.len())
                .position(|window| window == leap_15);
            if search_leap.is_some() {
                leap_15_found = true;
            } else {
                let search_centos = s[start + 2..]
                    .windows(centos_8.len())
                    .position(|window| window == centos_8);
                if search_centos.is_some() {
                    el_8_found = true;
                } else {
                    let search_el = s[start + 2..]
                        .windows(el_8.len())
                        .position(|window| window == el_8);
                    if search_el.is_some() {
                        el_8_found = true;
                    }
                }
            }
            {
                let mut data = OHPC2_RESULTS.write().unwrap();
                let mut year_found = false;
                for result in &*data {
                    if result.year == year {
                        year_found = true;
                        break;
                    }
                }
                if !year_found {
                    data.push(ResultOHPC2 {
                        year,
                        sles: 0,
                        rhel: 0,
                    });
                }
                for result in data.as_mut_slice() {
                    if result.year == year {
                        if el_8_found {
                            result.rhel += 1;
                        }
                        if leap_15_found {
                            result.sles += 1;
                        }
                        break;
                    }
                }
            }
        }
        let substring_3 = "3/".as_bytes();
        if &s[start..start + 2] == substring_3 {
            OHPC_3.fetch_add(1, Ordering::SeqCst);
            ohpc_3 = true;
            update_distributions_ohpc_3(&s[start + 2..], year);
        }
    }

    let size: u64 = match elements.len() < 10 {
        true => 0,
        false => match elements[9].parse::<u64>() {
            Ok(y) => y,
            _ => 0,
        },
    };

    let ip = match elements[0].parse::<IpAddr>() {
        Ok(ip) => ip,
        _ => IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
    };

    {
        let mut data = OVERALL_RESULTS.write().unwrap();
        let mut year_found = false;
        for result in &*data {
            if result.year == year {
                year_found = true;
                break;
            }
        }
        if !year_found {
            data.push(ResultOverall {
                year,
                ohpc_1: match ohpc_1 {
                    true => 1,
                    false => 0,
                },
                ohpc_2: match ohpc_2 {
                    true => 1,
                    false => 0,
                },
                ohpc_3: match ohpc_3 {
                    true => 1,
                    false => 0,
                },
                overall: 1,
                unique_ohpc_1: match ohpc_1 {
                    true => 1,
                    false => 0,
                },
                unique_ohpc_2: match ohpc_2 {
                    true => 1,
                    false => 0,
                },
                unique_ohpc_3: match ohpc_3 {
                    true => 1,
                    false => 0,
                },
                unique_overall: 1,
                size,
                ipv4: match &ip {
                    IpAddr::V4(ipv4) => vec![(*ipv4).into()].into_iter().collect(),
                    _ => vec![0].into_iter().collect(),
                },
                ipv6: match ip {
                    IpAddr::V6(ipv6) => vec![ipv6.into()].into_iter().collect(),
                    _ => vec![0].into_iter().collect(),
                },
            });
        } else {
            for result in data.as_mut_slice() {
                if result.year == year {
                    result.overall += 1;
                    result.size += size;
                    if ohpc_1 {
                        result.ohpc_1 += 1;
                    }
                    if ohpc_2 {
                        result.ohpc_2 += 1;
                    }
                    if ohpc_3 {
                        result.ohpc_3 += 1;
                    }
                    let mut unique = false;
                    match ip {
                        IpAddr::V4(ipv4) => {
                            if !result.ipv4.contains(&ipv4.into()) {
                                result.ipv4.insert(ipv4.into());
                                unique = true;
                            }
                        }
                        IpAddr::V6(ipv6) => {
                            if !result.ipv6.contains(&ipv6.into()) {
                                result.ipv6.insert(ipv6.into());
                                unique = true;
                            }
                        }
                    }
                    if unique {
                        result.unique_overall += 1;
                        if ohpc_1 {
                            result.unique_ohpc_1 += 1;
                        }
                        if ohpc_2 {
                            result.unique_ohpc_2 += 1;
                        }
                        if ohpc_3 {
                            result.unique_ohpc_3 += 1;
                        }
                    }
                    break;
                }
            }
        }
    }
    {
        let mut data_year_month = OVERALL_RESULTS_PER_MONTH.write().unwrap();
        let mut year_month_found = false;

        for result in &*data_year_month {
            if result.year == year && result.month == month {
                year_month_found = true;
                break;
            }
        }
        if !year_month_found {
            data_year_month.push(ResultOverallPerMonth {
                year,
                month,
                ohpc_1: match ohpc_1 {
                    true => 1,
                    false => 0,
                },
                ohpc_2: match ohpc_2 {
                    true => 1,
                    false => 0,
                },
                ohpc_3: match ohpc_3 {
                    true => 1,
                    false => 0,
                },
                overall: 1,
                unique_ohpc_1: match ohpc_1 {
                    true => 1,
                    false => 0,
                },
                unique_ohpc_2: match ohpc_2 {
                    true => 1,
                    false => 0,
                },
                unique_ohpc_3: match ohpc_3 {
                    true => 1,
                    false => 0,
                },
                unique_overall: 1,
                size,
                ipv4: match &ip {
                    IpAddr::V4(ipv4) => HashMap::from([((*ipv4).into(), 1)]),
                    _ => HashMap::from([(0, 0)]),
                },
                ipv6: match ip {
                    IpAddr::V6(ipv6) => HashMap::from([(ipv6.into(), 1)]),
                    _ => HashMap::from([(0, 0)]),
                },
            })
        } else {
            for result in data_year_month.as_mut_slice() {
                if result.year == year && result.month == month {
                    result.overall += 1;
                    result.size += size;
                    if ohpc_1 {
                        result.ohpc_1 += 1;
                    }
                    if ohpc_2 {
                        result.ohpc_2 += 1;
                    }
                    if ohpc_3 {
                        result.ohpc_3 += 1;
                    }
                    let mut unique = false;
                    match ip {
                        IpAddr::V4(ipv4) => {
                            if let std::collections::hash_map::Entry::Vacant(e) =
                                result.ipv4.entry(ipv4.into())
                            {
                                e.insert(1);
                                unique = true;
                            } else {
                                *result.ipv4.get_mut(&ipv4.into()).unwrap() += 1;
                            }
                        }
                        IpAddr::V6(ipv6) => {
                            if let std::collections::hash_map::Entry::Vacant(e) =
                                result.ipv6.entry(ipv6.into())
                            {
                                e.insert(1);
                                unique = true;
                            } else {
                                *result.ipv6.get_mut(&ipv6.into()).unwrap() += 1;
                            }
                        }
                    }
                    if unique {
                        result.unique_overall += 1;
                        if ohpc_1 {
                            result.unique_ohpc_1 += 1;
                        }
                        if ohpc_2 {
                            result.unique_ohpc_2 += 1;
                        }
                        if ohpc_3 {
                            result.unique_ohpc_3 += 1;
                        }
                    }
                    break;
                }
            }
        }
    }
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

const CHUNK_SIZE: usize = 100_000_000;

fn main() {
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

    for input in params.access_log.clone().into_iter() {
        let pb = ProgressBar::new(0);
        pb.set_style(spinner_style.clone());
        let mut access_log = match std::fs::File::open(input.clone()) {
            Ok(a) => a,
            Err(e) => {
                println!("Opening input file '{}' failed: {}", input, e);
                process::exit(1);
            }
        };
        print_step(format!("Using '{:}' as input", input));

        pool.scope(|scope| {
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

                loop {
                    // Do not spawn any more threads if the queue is already full.
                    if !pool.current_thread_has_pending_tasks().unwrap() {
                        break;
                    }
                }

                // Move our string into a rayon thread.
                let data = s;
                scope.spawn(move |_| {
                    let re = Regex::new(r"(.*GET.*){2,}").unwrap();
                    let d_s = data[..last_newline].split(|c| *c == b'\n');

                    for i in d_s {
                        let mut search = i.windows(3).position(|window| window == b"png");
                        if search.is_some() {
                            continue;
                        }
                        search = i.windows(3).position(|window| window == b"gif");
                        if search.is_some() {
                            continue;
                        }
                        if re.is_match(i) {
                            // Skip broken lines with two or more "GET"s
                            continue;
                        }
                        process_line(i);
                    }
                });
                s = next_s;
            }
            pb.finish();
        });
    }
    if let Err(e) = create_plots(params) {
        println!("Error creating diagrams: {}", e);
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
        Bar::new(
            labels,
            vec![
                OVERALL.load(Ordering::SeqCst)
                    - OHPC_1.load(Ordering::SeqCst)
                    - OHPC_2.load(Ordering::SeqCst)
                    - OHPC_3.load(Ordering::SeqCst),
            ],
        )
        .name("Other"),
    );
    plot.set_layout(
        Layout::new()
            .bar_mode(BarMode::Stack)
            .title("OHPC overall repository accesses".into()),
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
            .title("OHPC file types per year".into())
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
    let mut overall: Vec<i64> = Vec::new();
    let mut ticks: Vec<f64> = Vec::new();
    let data = OVERALL_RESULTS.read()?;

    for year in years {
        for result in &*data {
            if result.year == *year {
                ohpc_1.push(result.ohpc_1);
                ohpc_2.push(result.ohpc_2);
                ohpc_3.push(result.ohpc_3);
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
    plot.add_trace(Scatter::new(years.clone(), overall).name("Total"));
    plot.set_layout(
        Layout::new()
            .title("OHPC repository requests per year".into())
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
    let mut unique_overall: Vec<i64> = Vec::new();
    let mut ticks: Vec<f64> = Vec::new();

    let data = OVERALL_RESULTS.read()?;
    for year in years {
        for result in &*data {
            if result.year == *year {
                unique_ohpc_1.push(result.unique_ohpc_1);
                unique_ohpc_2.push(result.unique_ohpc_2);
                unique_ohpc_3.push(result.unique_ohpc_3);
                unique_overall.push(result.unique_overall);
                ticks.push((*year) as f64);
                json.unique_visitors_per_year.push(UniqueVisitorsPerYear {
                    year: *year,
                    ohpc1: result.unique_ohpc_1,
                    ohpc2: result.unique_ohpc_2,
                    ohpc3: result.unique_ohpc_3,
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
    let trace_unique_overall = Scatter::new(years.clone(), unique_overall).name("Total");
    unique_plot.add_trace(trace_unique_ohpc_1);
    unique_plot.add_trace(trace_unique_ohpc_2);
    unique_plot.add_trace(trace_unique_ohpc_3);
    unique_plot.add_trace(trace_unique_overall);
    let unique_layout = Layout::new()
        .title("Unique OHPC repository requests per year".into())
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
        .title("OHPC data downloaded per year".into())
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
        .add_trace(Scatter::new(year_months.clone(), overall_per_month).name("Total"));
    plot_overall_per_month
        .set_layout(Layout::new().title("OHPC repository requests per month".into()));

    Ok(plot_overall_per_month.to_inline_html(None))
}

fn create_unique_repository_requests_per_month(
    params: &Args,
    json: &mut Json,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut unique_ohpc_1_per_month: Vec<i64> = Vec::new();
    let mut unique_ohpc_2_per_month: Vec<i64> = Vec::new();
    let mut unique_ohpc_3_per_month: Vec<i64> = Vec::new();
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
                unique_overall_per_month.push(result.unique_overall);
                json.unique_visitors_per_month.push(UniqueVisitorsPerMonth {
                    year_month: year_month.clone(),
                    ohpc1: result.unique_ohpc_1,
                    ohpc2: result.unique_ohpc_2,
                    ohpc3: result.unique_ohpc_3,
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
    plot.add_trace(Scatter::new(year_months.clone(), unique_overall_per_month).name("Total"));
    plot.set_layout(Layout::new().title("Unique OHPC repository requests per month".into()));

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
            .title("OHPC repository requests per year and distribution".into())
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
    plot.set_layout(Layout::new().title("OHPC data downloaded per month".into()));

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

    let data = OVERALL_RESULTS_PER_MONTH.read()?;

    for result in &*data {
        result.ipv4.par_iter().for_each(|(key, value)| {
            let client_country = match geoip_reader
                .lookup::<maxminddb::geoip2::Country>(std::net::IpAddr::V4(Ipv4Addr::from(*key)))
            {
                Ok(c) => match c.country {
                    Some(co) => match co.iso_code {
                        Some(iso) => iso.to_string(),
                        _ => "N/A".to_string(),
                    },
                    _ => "N/A".to_string(),
                },
                _ => "N/A".to_string(),
            };

            let mut country_results = COUNTRY_RESULTS.write().unwrap();

            let mut found = false;
            for country_result in country_results.as_mut_slice() {
                if country_result.year == result.year
                    && country_result.country == client_country
                    && country_result.month == result.month
                {
                    country_result.count += *value;
                    found = true;
                    break;
                }
            }
            if !found {
                country_results.push(ResultCountry {
                    year: result.year,
                    month: result.month,
                    country: client_country,
                    count: *value,
                });
            }
        });
        result.ipv6.par_iter().for_each(|(key, value)| {
            let client_country = match geoip_reader
                .lookup::<maxminddb::geoip2::Country>(std::net::IpAddr::V6(Ipv6Addr::from(*key)))
            {
                Ok(c) => match c.country {
                    Some(co) => match co.iso_code {
                        Some(iso) => iso.to_string(),
                        _ => "N/A".to_string(),
                    },
                    _ => "N/A".to_string(),
                },
                _ => "N/A".to_string(),
            };

            let mut country_results = COUNTRY_RESULTS.write().unwrap();

            let mut found = false;
            for country_result in country_results.as_mut_slice() {
                if country_result.year == result.year
                    && country_result.country == client_country
                    && country_result.month == result.month
                {
                    country_result.count += *value;
                    found = true;
                    break;
                }
            }
            if !found {
                country_results.push(ResultCountry {
                    year: result.year,
                    month: result.month,
                    country: client_country,
                    count: *value,
                });
            }
        });
    }

    Ok(())
}

fn create_country_per_year_and_month(
    params: &Args,
    json: &mut Json,
) -> Result<String, Box<dyn std::error::Error>> {
    fill_country_results(params)?;

    let mut year_months: Vec<String> = Vec::new();
    let mut countries: Vec<String> = Vec::new();

    let data = COUNTRY_RESULTS.read()?;
    for result in &*data {
        year_months.push(format!("{}-{:02}", result.year, result.month));
        countries.push(result.country.clone());
    }
    year_months.sort_unstable();
    year_months.dedup();
    countries.sort_unstable();
    countries.dedup();

    #[derive(Debug)]
    struct CountryTraceResults {
        year_months: Vec<String>,
        count: Vec<i64>,
    }

    let mut country_results: HashMap<String, CountryTraceResults> = HashMap::new();

    for country in &countries {
        for year_month in &year_months {
            for result in &*data {
                if format!("{}-{:02}", result.year, result.month) == (*year_month).clone()
                    && result.country == *country
                {
                    let values = match country_results.entry((*country.clone()).to_string()) {
                        Entry::Occupied(o) => o.into_mut(),
                        Entry::Vacant(v) => v.insert(CountryTraceResults {
                            year_months: Vec::new(),
                            count: Vec::new(),
                        }),
                    };
                    values.year_months.push((*year_month).clone());
                    values.count.push(result.count);
                    json.result_country.push(ResultCountry {
                        year: result.year,
                        month: result.month,
                        country: (*country.clone()).to_string(),
                        count: result.count,
                    });
                }
            }
        }
    }

    let mut plot = Plot::new();
    let layout_country = Layout::new().title("OHPC repository country requests per month".into());
    plot.set_layout(layout_country);

    for result in country_results.keys() {
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

    let mut libdnf_results: HashMap<String, LibdnfTraceResults> = HashMap::new();

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
        .title("OHPC repository libdnf requests per year and distribution".into())
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

fn create_plots(params: Args) -> Result<(), Box<dyn std::error::Error>> {
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
    file.write_all(HTML_FOOTER.as_bytes())?;

    let mut writer = std::io::BufWriter::new(File::create(
        Path::new(&params.output_directory).join("stats.json"),
    )?);
    serde_json::to_writer(&mut writer, &json)?;
    writer.flush()?;

    Ok(())
}
