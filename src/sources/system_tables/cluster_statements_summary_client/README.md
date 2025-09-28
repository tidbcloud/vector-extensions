# TiDB CLUSTER_STATEMENTS_SUMMARY gRPC Coprocessor Client

This project demonstrates how to directly query TiDB cluster's `CLUSTER_STATEMENTS_SUMMARY` data through the gRPC coprocessor protocol, simulating TiDB's internal mechanism for handling cluster table queries.

## Project Features

- 🚀 **Direct gRPC Communication**: Bypasses MySQL protocol, directly uses TiDB's internal coprocessor protocol
- 🔍 **Real Process Simulation**: Completely simulates TiDB's internal process for handling `CLUSTER_STATEMENTS_SUMMARY` queries
- 🌐 **Parallel Queries**: Sends requests to all TiDB nodes in the cluster simultaneously
- 📊 **Data Merging**: Automatically merges results from different nodes and adds instance identifiers
- ⚡ **High Performance**: Avoids MySQL protocol overhead, directly accesses memory data

## Technical Principles

### TiDB Internal Mechanism

When executing `SELECT * FROM CLUSTER_STATEMENTS_SUMMARY`, TiDB internally:

1. **Node Discovery**: Gets all TiDB node information from etcd
2. **Task Distribution**: Creates coprocessor tasks for each node
3. **Parallel Requests**: Sends requests to each node's StatusPort via gRPC
4. **Data Collection**: Each node returns local statement statistics data from memory
5. **Result Merging**: Adds instance address to each row and merges final results

### Differences from Traditional Approach

| Aspect | Traditional MySQL Query | gRPC Coprocessor |
|--------|------------------------|------------------|
| **Protocol** | MySQL Protocol | gRPC protobuf |
| **Port** | 4000 (MySQL) | 10080 (StatusPort) |
| **Data Path** | SQL parsing→executor→result set | Direct memory access |
| **Performance** | Protocol conversion overhead | Zero-copy, high performance |
| **Concurrency** | Single connection serial | Multiple connection parallel |

## Build and Run

### 1. Dependency Management

```bash
cd cluster_statements_summary_client
go mod tidy
```

### 2. Build

```bash
go build -o cluster_client cluster_statements_summary_client.go
```

### 3. Run

```bash
./cluster_client
```

## Configuration

### Server Configuration

Configure your TiDB cluster information in the `GetTiDBServersFromEtcd()` function in `main()`:

```go
return []ServerInfo{
    {
        ServerType: "tidb",
        Address:    "127.0.0.1:4000",    // MySQL port
        StatusAddr: "127.0.0.1:10080",   // gRPC port  
        StatusPort: 10080,
        IP:         "127.0.0.1",
    },
    // Add more nodes...
}
```

### Important Port Information

- **MySQL Port (4000)**: Used for regular SQL queries
- **Status Port (10080)**: Used for gRPC coprocessor requests, this is the port used by this client

## Code Structure

```
cluster_statements_summary_client.go
├── ServerInfo                          # Server information structure
├── ClusterStatementsSummaryClient      # Main client class
├── QueryClusterStatementsSummary()     # Parallel query entry point
├── queryServerViaCoprocessor()         # Single node query
├── buildCoprocessorRequest()           # Build DAG request
├── parseCoprocessorResponse()          # Parse response data
└── mergeResults()                      # Merge results
```

## Example Output

```
=== TiDB CLUSTER_STATEMENTS_SUMMARY gRPC Coprocessor Client ===

Found 2 TiDB servers:
  - MySQL port: 127.0.0.1:4000, gRPC port: 127.0.0.1:10080
  - MySQL port: 127.0.0.1:4001, gRPC port: 127.0.0.1:10081

Starting CLUSTER_STATEMENTS_SUMMARY query via gRPC coprocessor...

Querying server: 127.0.0.1:4000
Querying server: 127.0.0.1:4001

Server 127.0.0.1:4000 returned 4 rows
  Row 1: Instance=127.0.0.1:4000, DigestText=SELECT * FROM table_1, ExecCount=100, TotalTime=1000
  Row 2: Instance=127.0.0.1:4000, DigestText=UPDATE table_1 SET col = ?, ExecCount=50, TotalTime=800
  Row 3: Instance=127.0.0.1:4000, DigestText=SELECT * FROM table_2, ExecCount=110, TotalTime=1100

=== Merged Results ===
Successfully queried servers: 2
Failed servers: 0
Total rows: 8

=== Statistics by Server ===
Server 127.0.0.1:4000: 4 rows
Server 127.0.0.1:4001: 4 rows
```

## Technical Details

### DAG Request Construction

```go
dagReq := &tipb.DAGRequest{
    TimeZoneName: "UTC",
    EncodeType:   tipb.EncodeType_TypeDefault,
    User: &tipb.UserIdentity{
        UserName: "root",
        UserHost: "%",
    },
    Executors: []*tipb.Executor{
        {
            Tp: tipb.ExecType_TypeMemTableScan,
            MemTableScan: &tipb.MemTableScan{
                TableId: 1, // CLUSTER_STATEMENTS_SUMMARY
                Columns: [...], // Column definitions
            },
        },
        {
            Tp: tipb.ExecType_TypeLimit,
            Limit: &tipb.Limit{Limit: 10},
        },
    },
}
```

### Response Parsing

Response data format is `tipb.SelectResponse`, containing:
- `Chunks[]`: Data chunk arrays
- `Warnings[]`: Warning information
- `ExecutionSummaries[]`: Execution statistics

## Notes

1. **Network Connection**: Ensure access to TiDB nodes' StatusPort (default 10080)
2. **Permission Requirements**: Need permissions to access STATEMENTS_SUMMARY
3. **Version Compatibility**: Use protobuf definitions compatible with target TiDB version
4. **Timeout Settings**: Adjust network timeout based on cluster size

## Extension Usage

This client framework can be extended to query other cluster tables:
- `CLUSTER_SLOW_QUERY`
- `CLUSTER_PROCESSLIST` 
- `CLUSTER_CONFIG`
- `CLUSTER_HARDWARE`
- `CLUSTER_LOAD`

Simply modify the table ID and column definitions in `buildCoprocessorRequest()`.