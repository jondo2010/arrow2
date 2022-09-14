use arrow_integration_testing::flight_server_scenarios;
use clap::Parser;

type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
type Result<T = (), E = Error> = std::result::Result<T, E>;

#[derive(Debug, Parser)]
#[clap(author, version, about("rust flight-test-perf-server"), long_about = None)]
struct Args {
    #[clap(long)]
    port: u16,
}

#[tokio::main]
async fn main() -> Result {
    #[cfg(feature = "logging")]
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    let port = args.port;

    flight_server_scenarios::perf::scenario_setup(port).await?;
    Ok(())
}
