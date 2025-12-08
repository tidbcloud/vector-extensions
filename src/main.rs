#[macro_use]
extern crate tracing;

use std::process::ExitCode;

use vector::{app::Application, extra_context::ExtraContext};
use prometheus_exporter::{
    self,
};

use prometheus::{
    IntCounter, IntGauge, Opts,
    core::{Collector, Desc},
    proto,
    register,
};

use std::time::{Instant, SystemTime, UNIX_EPOCH};

mod common;
mod sinks;
mod sources;
mod utils;

use std::{
    fs,
    io::{self, Error},
    iter::FromIterator,
};
use libc::c_int;
pub use libc::pid_t as Pid;
pub use procinfo::pid::{self, Stat as FullStat};

lazy_static::lazy_static! {
    // getconf CLK_TCK
    static ref CLOCK_TICK: i64 = {
        unsafe {
            libc::sysconf(libc::_SC_CLK_TCK)
        }
    };

    static ref PROCESS_ID: Pid = unsafe { libc::getpid() };
}
/// Gets the ID of the current process.
#[inline]
pub fn process_id() -> Pid {
    *PROCESS_ID
}
#[inline]
pub fn ticks_per_second() -> i64 {
    *CLOCK_TICK
}


/// A collector to collect process metrics.
pub struct ProcessCollector {
    descs: Vec<Desc>,
    cpu_total: IntCounter,
    vsize: IntGauge,
    rss: IntGauge,
    start_time: IntGauge,
}

impl ProcessCollector {
    pub fn new() -> Self {
        let mut descs = Vec::new();

        let cpu_total = IntCounter::with_opts(Opts::new(
            "process_cpu_seconds_total",
            "Total user and system CPU time spent in \
                 seconds.",
        ))
        .unwrap();
        descs.extend(cpu_total.desc().into_iter().cloned());

        let vsize = IntGauge::with_opts(Opts::new(
            "process_virtual_memory_bytes",
            "Virtual memory size in bytes.",
        ))
        .unwrap();
        descs.extend(vsize.desc().into_iter().cloned());

        let rss = IntGauge::with_opts(Opts::new(
            "process_resident_memory_bytes",
            "Resident memory size in bytes.",
        ))
        .unwrap();
        descs.extend(rss.desc().into_iter().cloned());

        let start_time = IntGauge::with_opts(Opts::new(
            "process_start_time_seconds",
            "Start time of the process since unix epoch \
                 in seconds.",
        ))
        .unwrap();
        descs.extend(start_time.desc().into_iter().cloned());

        Self {
            descs,
            cpu_total,
            vsize,
            rss,
            start_time,
        }
    }
}

impl Collector for ProcessCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        let p = match procfs::process::Process::myself() {
            Ok(p) => p,
            Err(..) => {
                // we can't construct a Process object, so there's no stats to gather
                return Vec::new();
            }
        };

        // memory
        self.vsize.set(p.stat().unwrap().vsize as i64);
        self.rss.set((p.stat().unwrap().rss * (*PAGESIZE as u64)) as i64);

        // cpu
        let cpu_total_mfs = {
            let total = (p.stat().unwrap().utime + p.stat().unwrap().stime) / ticks_per_second() as u64;
            let past = self.cpu_total.get();
            self.cpu_total.inc_by(total - past);

            self.cpu_total.collect()
        };

        // collect MetricFamilies.
        let mut mfs = Vec::with_capacity(4);
        mfs.extend(cpu_total_mfs);
        mfs.extend(self.vsize.collect());
        mfs.extend(self.rss.collect());
        mfs.extend(self.start_time.collect());
        mfs
    }
}

lazy_static::lazy_static! {
    // getconf PAGESIZE
    static ref PAGESIZE: i64 = {
        unsafe {
            libc::sysconf(libc::_SC_PAGESIZE)
        }
    };
}


#[cfg(unix)]
fn main() -> ExitCode {
    /* use vector::sinks::prometheus;


    let binding = "10.2.12.124:9184".parse().unwrap();
    let _exporter = prometheus_exporter::start(binding).unwrap();
    let pc = ProcessCollector::new();
    let _ = register(Box::new(pc)); */

    // Install the default crypto provider for Rustls
    // This is required for Rustls 0.23+ to avoid the panic about crypto provider selection
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("Failed to install default crypto provider");

    let exit_code = Application::run(ExtraContext::default())
        .code()
        .unwrap_or(exitcode::UNAVAILABLE) as u8;
    ExitCode::from(exit_code)
}
