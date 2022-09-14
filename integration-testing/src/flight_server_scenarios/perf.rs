use arrow2::{
    array::{Array, PrimitiveArray},
    chunk::Chunk,
    datatypes::{Field, Schema},
    io::{
        flight::{
            default_ipc_fields, deserialize_schemas, serialize_batch, serialize_schema,
            serialize_schema_to_info,
        },
        ipc::{self},
    },
};
use arrow_format::flight::data::*;
use arrow_format::flight::service::flight_service_server::*;
use futures::TryStreamExt;
use prost::Message;
use rand::Rng;
use tonic::{transport::Server, Request, Response, Status, Streaming};

use crate::perf::arrow::flight::{Perf, Token};

use super::{Result, TonicStream};

pub async fn scenario_setup(port: u16) -> Result {
    let addr = super::listen_on(port).await?;

    let service = FlightPerfService::new(format!("grpc+tcp://{}", addr));
    let svc = FlightServiceServer::new(service);
    let server = Server::builder().add_service(svc).serve(addr);

    // NOTE: Log output used in tests to signal server is ready
    println!("Server listening on {:?}", addr);
    server.await?;
    Ok(())
}

/// Create a chunk of `num_columns` of length `length` with random values.
fn get_perf_batches(num_columns: usize, length: usize) -> Chunk<Box<dyn Array>> {
    let mut rng = rand::thread_rng();
    Chunk::new(
        (0..num_columns)
            .map(|_seed| {
                PrimitiveArray::from_iter((0..length).map(|_| Some(rng.gen::<i64>()))).boxed()
            })
            .collect(),
    )
}

#[derive(Clone, Default)]
pub struct FlightPerfService {
    server_location: String,
    perf_schema: Schema,
}

impl FlightPerfService {
    pub fn new(server_location: String) -> Self {
        let perf_schema = Schema::from(vec![
            Field::new("a", arrow2::datatypes::DataType::Int64, false),
            Field::new("b", arrow2::datatypes::DataType::Int64, false),
            Field::new("c", arrow2::datatypes::DataType::Int64, false),
            Field::new("d", arrow2::datatypes::DataType::Int64, false),
        ]);

        Self {
            server_location,
            perf_schema,
        }
    }
}

#[tonic::async_trait]
impl FlightService for FlightPerfService {
    type HandshakeStream = TonicStream<Result<HandshakeResponse, Status>>;
    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake"))
    }

    type ListFlightsStream = TonicStream<Result<FlightInfo, Status>>;
    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights"))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        println!("get_flight_info: {:?}", request);
        let flight_descriptor = request.into_inner();
        let perf_request = Perf::decode(flight_descriptor.cmd.as_slice())
            .map_err(|e| Status::invalid_argument(format!("Invalid FlightDescriptor: {:?}", e)))?;
        dbg!(&perf_request);

        let endpoints: Vec<_> = (0..perf_request.stream_count as i64)
            .map(|i| {
                let token = Token {
                    definition: Some(perf_request.clone()),
                    start: i * perf_request.records_per_stream,
                    end: (i + 1) * perf_request.records_per_stream,
                };

                FlightEndpoint {
                    ticket: Some(Ticket {
                        ticket: token.encode_to_vec(),
                    }),
                    location: vec![Location {
                        uri: self.server_location.clone(),
                    }],
                }
            })
            .collect();

        let total_records = perf_request.stream_count as i64 * perf_request.records_per_stream;

        let serialized_schema = serialize_schema_to_info(&self.perf_schema, None)
            .map_err(|e| Status::unavailable(format!("Error serializing Schema: {:?}", e)))?;

        let info = FlightInfo {
            schema: serialized_schema,
            flight_descriptor: Some(flight_descriptor),
            endpoint: endpoints,
            total_records,
            total_bytes: -1,
        };

        Ok(Response::new(info))
    }
    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema"))
    }

    type DoGetStream = TonicStream<Result<FlightData, Status>>;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        let token = Token::decode(ticket.ticket.as_slice())
            .map_err(|e| Status::invalid_argument(format!("Error parsing ticket: {:?}", e)))?;

        println!("do_get: {:?}", token);

        let perf = token.definition.as_ref().unwrap();
        let num_full_flights = (perf.records_per_stream / (perf.records_per_batch as i64)) as usize;
        let full_flight_len = perf.records_per_batch as usize;
        let last_flight_len = (perf.records_per_stream % (perf.records_per_batch as i64)) as usize;

        let schema = serialize_schema(&self.perf_schema, None);
        let options = ipc::write::WriteOptions { compression: None };

        let full_chunk = get_perf_batches(4, full_flight_len);
        let (_, full_flight_data) = serialize_batch(
            &full_chunk,
            default_ipc_fields(&self.perf_schema.fields).as_slice(),
            &options,
        )
        .map_err(|e| Status::unknown(format!("Error serializing batch: {:?}", e)))?;

        let partial_chunk = get_perf_batches(4, last_flight_len);
        let (_, partial_flight_data) = serialize_batch(
            &partial_chunk,
            default_ipc_fields(&self.perf_schema.fields).as_slice(),
            &options,
        )
        .map_err(|e| Status::unknown(format!("Error serializing batch: {:?}", e)))?;

        let batches = std::iter::once(schema)
            .chain(std::iter::repeat(full_flight_data).take(num_full_flights))
            .chain(std::iter::once(partial_flight_data))
            .map(Ok);

        let output = futures::stream::iter(batches);
        Ok(Response::new(Box::pin(output) as Self::DoGetStream))
    }

    type DoPutStream = TonicStream<Result<PutResult, Status>>;

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        println!("do_put");
        let mut input_stream = request.into_inner();

        // Deserialize the schema from the first message in the stream.
        let flight_data = input_stream
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("Must send some FlightData"))?;
        let (schema, _ipc_schema) = deserialize_schemas(&flight_data.data_header)
            .map_err(|e| Status::invalid_argument(format!("Invalid schema: {:?}", e)))?;

        println!("{:?}", &schema);

        let stream = input_stream.map_ok(|flight_data| PutResult {
            app_metadata: flight_data.app_metadata,
        });
        Ok(Response::new(Box::pin(stream)))
    }

    type DoExchangeStream = TonicStream<Result<FlightData, Status>>;

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange"))
    }

    type DoActionStream = TonicStream<Result<arrow_format::flight::data::Result, Status>>;

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        println!("do_action: {:?}", request);
        let action = request.into_inner();
        if action.r#type == "ping" {
            let result = arrow_format::flight::data::Result { body: "ok".into() };
            let output = futures::stream::once(async { Ok(result) });
            Ok(Response::new(Box::pin(output)))
        } else {
            Err(Status::unimplemented(action.r#type))
        }
    }

    type ListActionsStream = TonicStream<Result<ActionType, Status>>;

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions"))
    }
}
