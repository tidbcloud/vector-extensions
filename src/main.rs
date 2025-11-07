#[macro_use]
extern crate tracing;

use std::process::ExitCode;

use vector::{app::Application, extra_context::ExtraContext};
use prometheus_exporter::{
    self,
    prometheus::register_counter,
};
use prometheus_client::{
    collector::Collector,
    encoding::{DescriptorEncoder, EncodeMetric},
    metrics::{
        counter::ConstCounter,
        gauge::{self, ConstGauge, Gauge},
        MetricType,
    },
    registry::{Registry, Unit},
};
use std::time::{Instant, SystemTime, UNIX_EPOCH};


mod common;
mod sinks;
mod sources;
mod utils;


/// Registers process metrics with the given registry. Note that the 'process_'
/// prefix is NOT added and should be specified by the caller if desired.
pub fn register(reg: &mut Registry) -> std::io::Result<()> {
    let start_time = Instant::now();
    let start_time_from_epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("process start time");

    #[cfg(target_os = "linux")]
    let system = linux::System::load()?;

    reg.register_with_unit(
        "start_time",
        "Time that the process started (in seconds since the UNIX epoch)",
        Unit::Seconds,
        ConstGauge::new(start_time_from_epoch.as_secs_f64()),
    );

    let clock_time_ts = Gauge::<f64, ClockMetric>::default();
    reg.register_with_unit(
        "clock_time",
        "Current system time for this process",
        Unit::Seconds,
        clock_time_ts,
    );

    reg.register_collector(Box::new(ProcessCollector {
        start_time,
        #[cfg(target_os = "linux")]
        system,
    }));

    Ok(())
}

#[derive(Debug)]
struct ProcessCollector {
    start_time: Instant,
    #[cfg(target_os = "linux")]
    system: linux::System,
}

impl Collector for ProcessCollector {
    fn encode(&self, mut encoder: DescriptorEncoder<'_>) -> std::fmt::Result {
        let uptime = ConstCounter::new(
            Instant::now()
                .saturating_duration_since(self.start_time)
                .as_secs_f64(),
        );
        let ue = encoder.encode_descriptor(
            "uptime",
            "Total time since the process started (in seconds)",
            Some(&Unit::Seconds),
            MetricType::Counter,
        )?;
        uptime.encode(ue)?;

        #[cfg(target_os = "linux")]
        self.system.encode(encoder)?;

        Ok(())
    }
}

// Metric that always reports the current system time on a call to [`get`].
#[derive(Copy, Clone, Debug, Default)]
struct ClockMetric;

impl gauge::Atomic<f64> for ClockMetric {
    fn inc(&self) -> f64 {
        self.get()
    }

    fn inc_by(&self, _v: f64) -> f64 {
        self.get()
    }

    fn dec(&self) -> f64 {
        self.get()
    }

    fn dec_by(&self, _v: f64) -> f64 {
        self.get()
    }

    fn set(&self, _v: f64) -> f64 {
        self.get()
    }

    fn get(&self) -> f64 {
        match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(elapsed) => elapsed.as_secs_f64().floor(),
            Err(e) => {
                tracing::warn!(
                    "System time is before the UNIX epoch; reporting negative timestamp"
                );
                -e.duration().as_secs_f64().floor()
            }
        }
    }
}

#[cfg(unix)]
fn main() -> ExitCode {
    use vector::sinks::prometheus;


    let binding = "127.0.0.1:9184".parse().unwrap();
    let _exporter = prometheus_exporter::start(binding).unwrap();
    let mut prom = prometheus_client::registry::Registry::default();
    if let Err(error) = register(prom.sub_registry_with_prefix("vector_process")) {

    }

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
