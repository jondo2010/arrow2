use std::time::Instant;

use arrow2::{
    datatypes::{Field, Schema},
    io::flight::{deserialize_batch, deserialize_schemas},
};
use arrow_format::flight::{
    data::{
        flight_descriptor::{self, DescriptorType},
        FlightDescriptor, FlightEndpoint,
    },
    service::flight_service_client::FlightServiceClient,
};
use arrow_integration_testing::perf::arrow::flight::{Perf, Token};
use clap::Parser;
use prost::Message;
use tonic::Request;

type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug, Parser)]
#[clap(author, version, about("Ported flight_benchmark from cpp"), long_about = None)]
struct Args {
    /// An existing performance server to benchmark against (leave blank to spawn one automatically)
    server_host: Option<String>,

    /// The port to connect to
    #[clap(default_value_t = 31337)]
    server_port: i32,

    /// An existing performance server listening on Unix socket (leave blank to spawn one automatically)");
    server_unix: Option<String>,

    /// Test Unix socket instead of TCP
    #[clap(default_value_t = false)]
    test_unix: bool,

    /// Number of times to run the perf test to increase precision
    #[clap(default_value_t = 1)]
    num_perf_runs: i32,

    /// Number of performance servers to run
    #[clap(default_value_t = 1)]
    num_servers: i32,

    /// Number of streams for each server
    #[clap(default_value_t = 4)]
    num_streams: i32,

    /// Number of concurrent gets
    #[clap(default_value_t = 4)]
    num_threads: i32,

    /// Total records per stream
    #[clap(default_value_t = 10000000)]
    records_per_stream: i64,

    /// Total records per batch within stream
    #[clap(default_value_t = 4096)]
    records_per_batch: i32,

    /// Test DoPut instead of DoGet
    #[clap(default_value_t = false)]
    test_put: bool,
}

struct PerformanceResult {
    num_batches: i64,
    num_records: i64,
    num_bytes: i64,
}

const QUANTILES: [f64; 3] = [0.5, 0.95, 0.99];
struct PerformanceStats {
    //std::mutex mutex;
    total_batches: i64,
    total_records: i64,
    total_bytes: i64,
    //mutable arrow::internal::TDigest latencies;
}

struct SizedBatch {
    //batch: arrow::RecordBatch,
    bytes: i64,
}

/*
fn get_put_data(token: &Token) -> Result<Vec<SizedBatch>, arrow2::error::Error> {
  /*
  if (!FLAGS_data_file.empty()) {
    ARROW_ASSIGN_OR_RAISE(auto file, arrow::io::ReadableFile::Open(FLAGS_data_file));
    ARROW_ASSIGN_OR_RAISE(auto reader,
                          arrow::ipc::RecordBatchFileReader::Open(std::move(file)));
    std::vector<SizedBatch> batches(reader->num_record_batches());
    for (int i = 0; i < reader->num_record_batches(); i++) {
      ARROW_ASSIGN_OR_RAISE(batches[i].batch, reader->ReadRecordBatch(i));
      RETURN_NOT_OK(arrow::ipc::GetRecordBatchSize(*batches[i].batch, &batches[i].bytes));
    }
    return batches;
  } */

  let schema = Schema::from(vec![
    Field::new("a", arrow2::datatypes::DataType::Int64, false),
    Field::new("b", arrow2::datatypes::DataType::Int64, false),
    Field::new("c", arrow2::datatypes::DataType::Int64, false),
    Field::new("d", arrow2::datatypes::DataType::Int64, false),
  ]);

  // This is hard-coded for right now, 4 columns each with int64
  let bytes_per_record = 32;

  //std::shared_ptr<ResizableBuffer> buffer;
  //std::vector<std::shared_ptr<Array>> arrays;

  let total_recoreds = token.definition.unwrap().records_per_stream;
  let length = token.definition.unwrap().records_per_batch;
  let ncolumns = 4;
  for i in 0..ncolumns {
    //RETURN_NOT_OK(MakeRandomByteBuffer(length * sizeof(int64_t), default_memory_pool(), &buffer, static_cast<int32_t>(i) /* seed */));
    //arrays.push_back(std::make_shared<Int64Array>(length, buffer));
    //RETURN_NOT_OK(arrays.back()->Validate());
  }

  std::shared_ptr<RecordBatch> batch = RecordBatch::Make(schema, length, arrays);
  std::vector<SizedBatch> batches;

  let records_sent = 0;
  while (records_sent < total_records) {
    if (records_sent + length > total_records) {
      let last_length = total_records - records_sent;
      // Hard-coded
      batches.push_back(SizedBatch{batch->Slice(0, last_length),
                                   /*bytes=*/last_length * bytes_per_record});
      records_sent += last_length;
    } else {
      // Hard-coded
      batches.push_back(SizedBatch{batch, /*bytes=*/length * bytes_per_record});
      records_sent += length;
    }
  }
  return batches;
}
*/
/*
async fn run_do_put_test<T>(
    client: &mut FlightServiceClient<T>,
    //const perf::Token& token,
    //const FlightEndpoint& endpoint,
    stats: &PerformanceStats,
) -> Result<PerformanceResult, arrow2::error::Error>
where
    T: tonic::client::GrpcService<tonic::body::BoxBody> + Clone,
    T::ResponseBody: tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
    <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError> + Send,
{
    //ARROW_ASSIGN_OR_RAISE(const auto batches, GetPutData(token));
    //StopWatch timer;
    let num_records = 0;
    let num_bytes = 0;

    let responses = client.do_put(Request::new(stream)).await?.into_inner();

    //ARROW_ASSIGN_OR_RAISE(
    //    auto do_put_result,
    //    client->DoPut(call_options, FlightDescriptor{}, batches[0].batch->schema()));
    //std::unique_ptr<FlightStreamWriter> writer = std::move(do_put_result.writer);
    //for (size_t i = 0; i < batches.size(); i++) {
    //  auto batch = batches[i];
    //  auto is_last = i == (batches.size() - 1);
    //  if (is_last) {
    //    RETURN_NOT_OK(writer->WriteRecordBatch(*batch.batch));
    //    num_records += batch.batch->num_rows();
    //    num_bytes += batch.bytes;
    //  } else {
    //    timer.Start();
    //    RETURN_NOT_OK(writer->WriteRecordBatch(*batch.batch));
    //    stats->AddLatency(timer.Stop());
    //    num_records += batch.batch->num_rows();
    //    num_bytes += batch.bytes;
    //  }
    //}
    //RETURN_NOT_OK(writer->Close());
    //return PerformanceResult{static_cast<int64_t>(batches.size()), num_records, num_bytes};
}
*/

/*
async fn run_do_get_test<T>(
  client: &mut FlightServiceClient<T>,
  //perf::Token& token,
  endpoint: &FlightEndpoint,
  //PerformanceStats* stats
) -> Result<PerformanceResult, arrow2::error::Error>
where
    T: tonic::client::GrpcService<tonic::body::BoxBody> + Clone,
    T::ResponseBody: tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
    <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError> + Send,
{
  let reader = client.do_get(Request::new(endpoint.ticket.unwrap())).await?;
  let mut reader = reader.into_inner();

  //FlightStreamChunk batch;

  // This is hard-coded for right now, 4 columns each with int64
  let bytes_per_record = 32;

  // This must also be set in perf_server.cc
  let verify = false;

  let mut num_bytes = 0;
  let mut num_records = 0;
  let mut num_batches = 0;
  //StopWatch timer;
  loop {
    //timer.Start();
    //ARROW_ASSIGN_OR_RAISE(batch, reader->Next());
    let batch = reader.message().await?;
    //stats->AddLatency(timer.Stop());

    if let Some(batch) = batch {
        let (schema, ipc_schema) = deserialize_schemas(&batch.data_header)?;

        let mut dictionaries = Default::default();

      let chunk = deserialize_batch(&batch, &schema.fields, &ipc_schema, dictionaries)?;
      if verify {
        //let values = batch.d
      }

    }

    if verify {
      auto values = batch.data->column_data(0)->GetValues<int64_t>(1);
      const int64_t start = token.start() + num_records;
      for (int64_t i = 0; i < batch.data->num_rows(); ++i) {
        if (values[i] != start + i) {
          return Status::Invalid("verification failure");
        }
      }
    }

    num_batches += 1;
    num_records += batch.data->num_rows();

    // Hard-coded
    num_bytes += batch.data->num_rows() * bytes_per_record;
  }
  return PerformanceResult{num_batches, num_records, num_bytes};
}
*/

async fn do_single_perf_run<T>(
    client: &mut FlightServiceClient<T>,
    stats: &mut PerformanceStats,
) -> Result<(), Error>
where
    T: tonic::client::GrpcService<tonic::body::BoxBody> + Clone,
    T::ResponseBody: tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
    <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError> + Send,
{
    // Schema not needed
    let perf = Perf {
        stream_count: 4,
        records_per_stream: 10000000,
        records_per_batch: 4096,
        ..Default::default()
    };

    // Plan the query
    let mut descriptor = FlightDescriptor {
        r#type: DescriptorType::Cmd as i32,
        ..Default::default()
    };
    perf.encode(&mut descriptor.cmd)?;

    let plan = client.get_flight_info(Request::new(descriptor)).await?;

    dbg!(plan);

    let start_total_records = stats.total_records;

    Ok(())
}

async fn run_performance_test<T>(
    args: &Args,
    client: &mut FlightServiceClient<T>,
) -> Result<(), Error>
where
    T: tonic::client::GrpcService<tonic::body::BoxBody> + Clone,
    T::ResponseBody: tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
    <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError> + Send,
{
    let mut stats = PerformanceStats {
        total_batches: 0,
        total_records: 0,
        total_bytes: 0,
    };

    let start = Instant::now();
    for _ in 0..args.num_perf_runs {
        do_single_perf_run(client, &mut stats).await?;
    }
    let elapsed = start.elapsed();

    println!("Number of perf runs: {}", args.num_perf_runs);
    println!("Number of concurrent gets/puts: {}", args.num_threads);
    println!("Batch size: {}", stats.total_bytes / stats.total_batches);
    if args.test_put {
        println!("Batches written: {}", stats.total_batches);
        println!("Bytes written: {}", stats.total_bytes);
    } else {
        println!("Batches read: {}", stats.total_batches);
        println!("Bytes read: {}", stats.total_bytes);
    }

    println!("Nanos: {}", elapsed.as_nanos());
    println!(
        "Speed: {} MB/s",
        stats.total_bytes as f64 / (1 << 20) as f64 / elapsed.as_secs() as f64
    );

    // Calculate throughput(IOPS) and latency vs batch size
    println!(
        "Throughput: {} batches/s",
        stats.total_batches as f64 / elapsed.as_secs() as f64
    );
    //println!("Latency mean: {} us", stats.mean_latency());
    for q in QUANTILES {
        //println!("Latency quantile={}: {} us", q, stats.quantile_latency(q));
    }
    //println!("Latency max: {} us", stats.max_latency());

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    #[cfg(feature = "logging")]
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    let url = format!(
        "http://{}:{}",
        args.server_host.clone().unwrap_or("localhost".into()),
        args.server_port
    );
    let mut client = FlightServiceClient::connect(url).await?;

    run_performance_test(&args, &mut client).await?;

    Ok(())
}
