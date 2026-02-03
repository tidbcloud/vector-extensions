// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Statement V3 push receiver for TiDB statement statistics.
//!
//! This module implements a gRPC server that receives aggregated statement
//! statistics from TiDB instances and stores them in S3 as Parquet files.

pub mod config;
pub mod grpc_server;
pub mod contract;
pub mod schema;
pub mod storage;
pub mod health;

pub use config::StatementConfig;
pub use grpc_server::StatementReceiver;
pub use health::{HealthChecker, HealthStatus, BackpressureState, BackpressureAction, RateLimiter};
