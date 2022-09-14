pub mod arrow {
    pub mod flight {
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct Perf {
            #[prost(bytes = "vec", tag = "1")]
            pub schema: ::prost::alloc::vec::Vec<u8>,
            #[prost(int32, tag = "2")]
            pub stream_count: i32,
            #[prost(int64, tag = "3")]
            pub records_per_stream: i64,
            #[prost(int32, tag = "4")]
            pub records_per_batch: i32,
        }
        ///
        /// Payload of ticket
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct Token {
            /// definition of entire flight.
            #[prost(message, optional, tag = "1")]
            pub definition: ::core::option::Option<Perf>,
            /// inclusive start
            #[prost(int64, tag = "2")]
            pub start: i64,
            /// exclusive end
            #[prost(int64, tag = "3")]
            pub end: i64,
        }
    }
}
