extern crate regex;

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::{env, process, str};

use getopts::Options;
use plotly::layout::{Axis, BarMode, Layout};
use plotly::{Bar, Plot, Scatter};
use regex::bytes::Regex;

static OVERALL: AtomicUsize = AtomicUsize::new(0);
static OHPC_1: AtomicUsize = AtomicUsize::new(0);
static OHPC_2: AtomicUsize = AtomicUsize::new(0);
static OHPC_3: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug)]
struct ResultOverall {
    year: i64,
    ohpc_1: i64,
    ohpc_2: i64,
    ohpc_3: i64,
    overall: i64,
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
struct ResultLIBDNF {
    year: i64,
    name: String,
    count: i64,
}

#[derive(Debug)]
struct ResultType {
    year: i64,
    tar: i64,
    rpm: i64,
    repomd_xml: i64,
}

static OVERALL_RESULTS: RwLock<Vec<ResultOverall>> = RwLock::new(Vec::new());
static OHPC1_RESULTS: RwLock<Vec<ResultOHPC1>> = RwLock::new(Vec::new());
static OHPC2_RESULTS: RwLock<Vec<ResultOHPC2>> = RwLock::new(Vec::new());
static OHPC3_RESULTS: RwLock<Vec<ResultOHPC3>> = RwLock::new(Vec::new());
static LIBDNF_RESULTS: RwLock<Vec<ResultLIBDNF>> = RwLock::new(Vec::new());
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

fn count_type(elements: &Vec<String>, year: i64) {
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

fn count_libdnf(elements: &Vec<String>, year: i64) {
    if elements.len() < 12 {
        return;
    }
    if elements[11] == "\"libdnf" {
        let user_agent_long = elements[12..].join(" ");
        let user_agent_vec: Vec<_> = user_agent_long.split(';').map(|s| s.to_string()).collect();
        let mut user_agent_short = user_agent_vec[0].clone();
        user_agent_short.truncate(user_agent_short.rfind(' ').unwrap());
        let user_agent = &user_agent_short[1..];
        if user_agent.is_empty() {
            return;
        }
        let mut data = LIBDNF_RESULTS.write().unwrap();
        let mut name_and_year_found = false;
        for result in data.as_mut_slice() {
            if result.year == year && result.name == user_agent {
                name_and_year_found = true;
                result.count += 1;
                break;
            }
        }
        if !name_and_year_found {
            data.push(ResultLIBDNF {
                year,
                name: user_agent.to_string(),
                count: 1,
            });
        }
    }
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

fn process_line(s: &[u8]) {
    let line = str::from_utf8(s).unwrap();
    let elements: Vec<_> = line.split(' ').map(|s| s.to_string()).collect();
    OVERALL.fetch_add(1, Ordering::SeqCst);
    if elements.len() < 3 {
        // Skip incomplete lines
        return;
    }
    let year = elements[3][8..12].parse::<i64>().unwrap();
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
            ohpc_1: 0,
            ohpc_2: 0,
            ohpc_3: 0,
            overall: 0,
        })
    }
    for result in data.as_mut_slice() {
        if result.year == year {
            result.overall += 1;
            if ohpc_1 {
                result.ohpc_1 += 1;
            }
            if ohpc_2 {
                result.ohpc_2 += 1;
            }
            if ohpc_3 {
                result.ohpc_3 += 1;
            }
            break;
        }
    }
}

const CHUNK_SIZE: usize = 100_000_000;

struct Parameters {
    access_log: String,
    repomd_log: String,
    aarch64_log: String,
    x86_64_log: String,
    overall_log: String,
    html_output: String,
}

fn setup_params() -> Parameters {
    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();
    let mut opts = Options::new();
    let mut params = Parameters {
        access_log: String::new(),
        repomd_log: String::from("repomd.log"),
        aarch64_log: String::from("aarch64.log"),
        x86_64_log: String::from("x86_64.log"),
        overall_log: String::from("overall.log"),
        html_output: String::from("ohpc-statistics.html"),
    };

    opts.optmulti("", "access-log", "input file (httpd log file)", "");
    opts.optmulti(
        "",
        "repomd",
        &format!("output file - only repomd accesses ({})", params.repomd_log),
        "",
    );

    opts.optmulti(
        "",
        "aarch64",
        &format!(
            "output file - only aarch64 accesses ({})",
            params.aarch64_log
        ),
        "",
    );

    opts.optmulti(
        "",
        "x86-64",
        &format!("output file - only x86_64 accesses ({})", params.x86_64_log),
        "",
    );

    opts.optmulti(
        "",
        "overall",
        &format!("output file - all accesses ({})", params.overall_log),
        "",
    );

    opts.optmulti(
        "",
        "html-output",
        &format!("html output file ({})", params.html_output),
        "",
    );

    let matches = match opts.parse(&args[1..]) {
        Ok(m) => m,
        _ => {
            print!(
                "{}",
                opts.usage(format!("Usage: {} [options]", program).as_str())
            );
            process::exit(0);
        }
    };

    if matches.opt_present("access-log") {
        params.access_log =
            matches.opt_strs("access-log")[matches.opt_count("access-log") - 1].to_string();
    }

    if matches.opt_present("repomd") {
        params.repomd_log = matches.opt_strs("repomd")[matches.opt_count("repomd") - 1].to_string();
    }

    if matches.opt_present("aarch64") {
        params.aarch64_log =
            matches.opt_strs("aarch64")[matches.opt_count("aarch64") - 1].to_string();
    }

    if matches.opt_present("x86-64") {
        params.x86_64_log = matches.opt_strs("x86-64")[matches.opt_count("x86-64") - 1].to_string();
    }

    if matches.opt_present("overall") {
        params.overall_log =
            matches.opt_strs("overall")[matches.opt_count("overall") - 1].to_string();
    }

    if matches.opt_present("html-output") {
        params.html_output =
            matches.opt_strs("html-output")[matches.opt_count("html-output") - 1].to_string();
    }

    params
}

fn main() {
    let params = setup_params();
    let pool = rayon::ThreadPoolBuilder::new().build().unwrap();
    let repomd_xml_log_file: Arc<Mutex<BufWriter<File>>> = Arc::new({
        let file = match File::create(params.repomd_log.clone()) {
            Ok(r) => r,
            Err(e) => {
                println!(
                    "Creating repomd output file '{}' failed: {}",
                    params.repomd_log, e
                );
                process::exit(1);
            }
        };
        BufWriter::new(file).into()
    });
    let aarch64_log_file: Arc<Mutex<BufWriter<File>>> = Arc::new({
        let file = match File::create(params.aarch64_log.clone()) {
            Ok(r) => r,
            Err(e) => {
                println!(
                    "Creating aarch64 output file '{}' failed: {}",
                    params.aarch64_log, e
                );
                process::exit(1);
            }
        };
        BufWriter::new(file).into()
    });

    let x86_64_log_file: Arc<Mutex<BufWriter<File>>> = Arc::new({
        let file = match File::create(params.x86_64_log.clone()) {
            Ok(r) => r,
            Err(e) => {
                println!(
                    "Creating x86_64 output file '{}' failed: {}",
                    params.x86_64_log, e
                );
                process::exit(1);
            }
        };
        BufWriter::new(file).into()
    });

    let overall_log_file: Arc<Mutex<BufWriter<File>>> = Arc::new({
        let file = match File::create(params.overall_log.clone()) {
            Ok(r) => r,
            Err(e) => {
                println!(
                    "Creating overall output file '{}' failed: {}",
                    params.overall_log, e
                );
                process::exit(1);
            }
        };
        BufWriter::new(file).into()
    });

    let mut access_log = match std::fs::File::open(params.access_log.clone()) {
        Ok(a) => a,
        Err(e) => {
            println!("Opening input file '{}' failed: {}", params.access_log, e);
            process::exit(1);
        }
    };
    println!("Using '{}' as input", params.access_log);
    println!("Using '{}' as repomd output", params.repomd_log);
    println!("Using '{}' as aarch64 output", params.aarch64_log);
    println!("Using '{}' as x86_64 output", params.x86_64_log);
    println!("Using '{}' as overall output", params.overall_log);
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

            // Copy any incomplete lines to the next s.
            let last_newline = last_newline(&s);
            let mut next_s = Vec::with_capacity(CHUNK_SIZE);
            next_s.extend_from_slice(&s[last_newline..]);
            s.truncate(last_newline);

            loop {
                // Do not spawn any more threads if the queue is already full.
                if !pool.current_thread_has_pending_tasks().unwrap() {
                    break;
                }
            }

            // Move our string into a rayon thread.
            let data = s;
            let repomd_xml_log_file = Arc::clone(&repomd_xml_log_file);
            let aarch64_log_file = Arc::clone(&aarch64_log_file);
            let x86_64_log_file = Arc::clone(&x86_64_log_file);
            let overall_log_file = Arc::clone(&overall_log_file);
            scope.spawn(move |_| {
                let re = Regex::new(r"\[\d{2}\/").unwrap();
                let mut repomd_xml_content = String::new();
                let mut aarch64_content = String::new();
                let mut x86_64_content = String::new();
                let mut overall_content = String::new();
                let d_s = data[..last_newline].split(|c| *c == b'\n');

                let substring_repomd_xml = "/repomd.xml".as_bytes();
                let substring_aarch64 = ".aarch64.".as_bytes();
                let substring_x86_64 = ".x86_64.".as_bytes();
                for i in d_s {
                    let mut search = i.windows(3).position(|window| window == b"png");
                    if search.is_some() {
                        continue;
                    }
                    search = i.windows(3).position(|window| window == b"gif");
                    if search.is_some() {
                        continue;
                    }
                    let j = re.replace_all(i, b"[01/");
                    search = j
                        .windows(substring_repomd_xml.len())
                        .position(|window| window == substring_repomd_xml);

                    if search.is_some() {
                        repomd_xml_content += str::from_utf8(&j).unwrap();
                        repomd_xml_content += "\n";
                    }

                    search = j
                        .windows(substring_aarch64.len())
                        .position(|window| window == substring_aarch64);

                    if search.is_some() {
                        aarch64_content += str::from_utf8(&j).unwrap();
                        aarch64_content += "\n";
                    }

                    search = j
                        .windows(substring_x86_64.len())
                        .position(|window| window == substring_x86_64);

                    if search.is_some() {
                        x86_64_content += str::from_utf8(&j).unwrap();
                        x86_64_content += "\n";
                    }

                    overall_content += str::from_utf8(&j).unwrap();
                    overall_content += "\n";

                    process_line(&j);
                }

                let mut log_file = repomd_xml_log_file.lock().unwrap();
                if let Err(e) = log_file.write_all(repomd_xml_content.as_bytes()) {
                    println!("Error writing to repomd specific log file: {}", e);
                }

                log_file = aarch64_log_file.lock().unwrap();
                if let Err(e) = log_file.write_all(aarch64_content.as_bytes()) {
                    println!("Error writing to aarch64 specific log file: {}", e);
                }

                log_file = x86_64_log_file.lock().unwrap();
                if let Err(e) = log_file.write_all(x86_64_content.as_bytes()) {
                    println!("Error writing to x86_64 specific log file: {}", e);
                }

                log_file = overall_log_file.lock().unwrap();
                if let Err(e) = log_file.write_all(overall_content.as_bytes()) {
                    println!("Error writing to overall log file: {}", e);
                }
            });
            s = next_s;
        }
    });
    if let Err(e) = create_plots(params.html_output) {
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

fn create_plots(output: String) -> Result<(), Box<dyn std::error::Error>> {
    let mut ohpc_1: Vec<i64> = Vec::new();
    let mut ohpc_2: Vec<i64> = Vec::new();
    let mut ohpc_3: Vec<i64> = Vec::new();
    let mut overall: Vec<i64> = Vec::new();
    let mut ticks: Vec<f64> = Vec::new();

    let mut years: Vec<i64> = Vec::new();
    let data = OVERALL_RESULTS.read().unwrap();
    for result in &*data {
        years.push(result.year);
    }
    years.sort_unstable();

    for year in &years {
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
    let trace_ohpc_1 = Scatter::new(years.clone(), ohpc_1).name("OHPC 1.3.x");
    let trace_ohpc_2 = Scatter::new(years.clone(), ohpc_2).name("OHPC 2.x");
    let trace_ohpc_3 = Scatter::new(years.clone(), ohpc_3).name("OHPC 3.x");
    let trace_overall = Scatter::new(years.clone(), overall).name("Total");
    plot.add_trace(trace_ohpc_1);
    plot.add_trace(trace_ohpc_2);
    plot.add_trace(trace_ohpc_3);
    plot.add_trace(trace_overall);
    let layout = Layout::new()
        .title("OHPC repository requests per year".into())
        .x_axis(Axis::new().tick_values(ticks.clone()));
    plot.set_layout(layout);
    let mut ohpc_1_sles: Vec<i64> = Vec::new();
    let mut ohpc_1_rhel: Vec<i64> = Vec::new();
    let data_ohpc_1 = OHPC1_RESULTS.read().unwrap();
    years = Vec::new();
    for result in &*data_ohpc_1 {
        years.push(result.year);
    }
    years.sort_unstable();
    for year in &years {
        for result in &*data_ohpc_1 {
            if result.year == *year {
                ohpc_1_sles.push(result.sles);
                ohpc_1_rhel.push(result.rhel);
                break;
            }
        }
    }
    let trace_ohpc_1_sles = Scatter::new(years.clone(), ohpc_1_sles).name("OHPC SLES 1.3.x");
    let trace_ohpc_1_rhel = Scatter::new(years.clone(), ohpc_1_rhel).name("OHPC RHEL 1.3.x");
    let mut plot_ohpc_1 = Plot::new();
    plot_ohpc_1.add_trace(trace_ohpc_1_sles);
    plot_ohpc_1.add_trace(trace_ohpc_1_rhel);
    let layout_ohpc_1 = Layout::new()
        .title("OHPC repository requests per year and distribution".into())
        .x_axis(Axis::new().tick_values(ticks.clone()));
    plot_ohpc_1.set_layout(layout_ohpc_1);

    let mut ohpc_2_sles: Vec<i64> = Vec::new();
    let mut ohpc_2_rhel: Vec<i64> = Vec::new();
    let data_ohpc_2 = OHPC2_RESULTS.read().unwrap();
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
    let trace_ohpc_2_sles = Scatter::new(years.clone(), ohpc_2_sles).name("OHPC SLES 2.x");
    let trace_ohpc_2_rhel = Scatter::new(years.clone(), ohpc_2_rhel).name("OHPC RHEL 2.x");
    plot_ohpc_1.add_trace(trace_ohpc_2_sles);
    plot_ohpc_1.add_trace(trace_ohpc_2_rhel);

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
    let trace_ohpc_3_sles = Scatter::new(years.clone(), ohpc_3_sles).name("OHPC SLES 3.x");
    let trace_ohpc_3_rhel = Scatter::new(years.clone(), ohpc_3_rhel).name("OHPC RHEL 3.x");
    let trace_ohpc_3_openeuler =
        Scatter::new(years.clone(), ohpc_3_openeuler).name("OHPC openEuler 3.x");
    plot_ohpc_1.add_trace(trace_ohpc_3_sles);
    plot_ohpc_1.add_trace(trace_ohpc_3_rhel);
    plot_ohpc_1.add_trace(trace_ohpc_3_openeuler);

    let data_libdnf = LIBDNF_RESULTS.read().unwrap();
    years = Vec::new();
    let mut distributions: Vec<String> = Vec::new();
    for result in &*data_libdnf {
        years.push(result.year);
        distributions.push(result.name.clone());
    }
    years.sort_unstable();
    years.dedup();
    distributions.sort_unstable();
    distributions.dedup();

    #[derive(Debug)]
    struct LibdnfTraceResults {
        years: Vec<i64>,
        count: Vec<i64>,
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

    {
        let mut file = File::create(output).unwrap();
        file.write_all(HTML_HEADER.as_bytes())?;
        file.write_all(plot.to_inline_html(None).as_bytes())?;
        file.write_all(plot_ohpc_1.to_inline_html(None).as_bytes())?;
        file.write_all(plot_libdnf.to_inline_html(None).as_bytes())?;
        file.write_all(create_overall_plot().as_bytes())?;
        file.write_all(create_type_plot().as_bytes())?;
        file.write_all(HTML_FOOTER.as_bytes())?;
    }

    Ok(())
}
