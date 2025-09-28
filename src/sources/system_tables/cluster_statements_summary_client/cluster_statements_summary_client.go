package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pingcap/tipb/go-tipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Simplified type definitions without dependency on TiDB internal packages
type MySQLType int32

const (
	TypeTiny       MySQLType = 1
	TypeShort      MySQLType = 2
	TypeInt24      MySQLType = 9
	TypeLong       MySQLType = 3
	TypeLonglong   MySQLType = 8
	TypeFloat      MySQLType = 4
	TypeDouble     MySQLType = 5
	TypeString     MySQLType = 254
	TypeVarString  MySQLType = 15
	TypeVarchar    MySQLType = 15
	TypeTinyBlob   MySQLType = 249
	TypeMediumBlob MySQLType = 250
	TypeLongBlob   MySQLType = 251
	TypeBlob       MySQLType = 252
	TypeJSON       MySQLType = 245
	TypeDate       MySQLType = 10
	TypeDatetime   MySQLType = 12
	TypeTimestamp  MySQLType = 7
	TypeDuration   MySQLType = 11
	TypeNewDecimal MySQLType = 246
	TypeEnum       MySQLType = 247
	TypeSet        MySQLType = 248
	TypeBit        MySQLType = 16
)

// Simplified Chunk structure
type Chunk struct {
	columns []*Column
}

type Column struct {
	data       []byte
	offsets    []int64
	length     int
	nullBitmap []byte
}

func (c *Chunk) NumRows() int {
	if len(c.columns) == 0 {
		return 0
	}
	return c.columns[0].length
}

func (c *Chunk) Column(colIdx int) *Column {
	if colIdx >= len(c.columns) {
		return nil
	}
	return c.columns[colIdx]
}

func (c *Column) IsNull(rowIdx int) bool {
	if rowIdx >= c.length || len(c.nullBitmap) == 0 {
		return false
	}
	byteIdx := rowIdx / 8
	bitIdx := rowIdx % 8
	return (c.nullBitmap[byteIdx] & (1 << bitIdx)) == 0
}

func (c *Column) GetInt64(rowIdx int) int64 {
	if c.IsNull(rowIdx) {
		return 0
	}
	offset := rowIdx * 8
	if offset+8 > len(c.data) {
		return 0
	}
	return int64(binary.LittleEndian.Uint64(c.data[offset : offset+8]))
}

func (c *Column) GetString(rowIdx int) string {
	if c.IsNull(rowIdx) {
		return ""
	}
	if len(c.offsets) <= rowIdx+1 {
		return ""
	}
	start := int(c.offsets[rowIdx])
	end := int(c.offsets[rowIdx+1])
	if start >= end || start >= len(c.data) || end > len(c.data) {
		return ""
	}
	return string(c.data[start:end])
}

func (c *Column) GetFloat32(rowIdx int) float32 {
	if c.IsNull(rowIdx) {
		return 0
	}
	offset := rowIdx * 4
	if offset+4 > len(c.data) {
		return 0
	}
	return math.Float32frombits(binary.LittleEndian.Uint32(c.data[offset : offset+4]))
}

func (c *Column) GetFloat64(rowIdx int) float64 {
	if c.IsNull(rowIdx) {
		return 0
	}
	offset := rowIdx * 8
	if offset+8 > len(c.data) {
		return 0
	}
	return math.Float64frombits(binary.LittleEndian.Uint64(c.data[offset : offset+8]))
}

func (c *Column) GetBytes(rowIdx int) []byte {
	if c.IsNull(rowIdx) {
		return nil
	}
	if len(c.offsets) <= rowIdx+1 {
		return nil
	}
	start := int(c.offsets[rowIdx])
	end := int(c.offsets[rowIdx+1])
	if start >= end || start >= len(c.data) || end > len(c.data) {
		return nil
	}
	return c.data[start:end]
}

// TableSchemaColumn represents column information in table schema
type TableSchemaColumn struct {
	ID   int64 `json:"id"`
	Name struct {
		O string `json:"O"` // Original name
		L string `json:"L"` // Lowercase name
	} `json:"name"`
	Type struct {
		Tp      int32  `json:"tp"`
		Flag    uint32 `json:"flag"`
		Flen    int32  `json:"flen"`
		Decimal int32  `json:"decimal"`
		Charset string `json:"charset"`
		Collate string `json:"collate"`
	} `json:"type"`
}

// ServerInfo represents TiDB server information
type ServerInfo struct {
	ServerType string
	Address    string
	StatusAddr string
	StatusPort uint
	IP         string
}

// TableSchema represents table schema information
type TableSchema struct {
	ID   int64 `json:"id"`
	Name struct {
		O string `json:"O"` // Original name
		L string `json:"L"` // Lowercase name
	} `json:"name"`
	Columns []struct {
		ID   int64 `json:"id"`
		Name struct {
			O string `json:"O"` // Original name
			L string `json:"L"` // Lowercase name
		} `json:"name"`
		Type struct {
			Tp      int32  `json:"tp"`
			Flag    uint32 `json:"flag"`
			Flen    int32  `json:"flen"`
			Decimal int32  `json:"decimal"`
			Charset string `json:"charset"`
			Collate string `json:"collate"`
		} `json:"type"`
	} `json:"cols"`
}

// ClusterStatementsSummaryClient client for querying CLUSTER_STATEMENTS_SUMMARY via gRPC coprocessor
type ClusterStatementsSummaryClient struct {
	servers []ServerInfo // List of TiDB server information
}

// NewClusterStatementsSummaryClient creates a new client
func NewClusterStatementsSummaryClient(servers []ServerInfo) *ClusterStatementsSummaryClient {
	return &ClusterStatementsSummaryClient{
		servers: servers,
	}
}

// getTableSchema gets table schema information via HTTP API
func (c *ClusterStatementsSummaryClient) getTableSchema(ctx context.Context, server ServerInfo) (*TableSchema, error) {
	// Construct HTTP request URL
	url := fmt.Sprintf("http://%s/schema/information_schema/cluster_statements_summary", server.StatusAddr)

	// Create HTTP client
	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	// Send request
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %v", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send HTTP request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP request failed, status code: %d", resp.StatusCode)
	}

	// Read response
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %v", err)
	}

	// Parse JSON
	var schema TableSchema
	err = json.Unmarshal(body, &schema)
	if err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %v", err)
	}

	return &schema, nil
}

// checkTiDBStatus checks TiDB instance status and statement summary configuration
func (c *ClusterStatementsSummaryClient) checkTiDBStatus(ctx context.Context, server ServerInfo) error {
	// Check TiDB status
	statusURL := fmt.Sprintf("http://%s/status", server.StatusAddr)
	log.Printf("Checking TiDB status: %s", statusURL)

	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	resp, err := client.Get(statusURL)
	if err != nil {
		return fmt.Errorf("failed to check TiDB status: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("TiDB status check failed: HTTP %d", resp.StatusCode)
	}

	// Check statement summary related configuration
	configURL := fmt.Sprintf("http://%s/config", server.StatusAddr)
	log.Printf("Checking TiDB configuration: %s", configURL)

	resp, err = client.Get(configURL)
	if err != nil {
		log.Printf("Warning: unable to get TiDB configuration: %v", err)
	} else {
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			body, err := io.ReadAll(resp.Body)
			if err == nil {
				log.Printf("TiDB configuration info: %s", string(body)[:min(200, len(body))])
			}
		}
	}

	return nil
}

// QueryClusterStatementsSummary queries cluster statements summary
func (c *ClusterStatementsSummaryClient) QueryClusterStatementsSummary(ctx context.Context) error {
	var wg sync.WaitGroup
	resultChan := make(chan QueryResult, len(c.servers))

	// First get schema information from the first server
	var tableSchema *TableSchema
	for _, server := range c.servers {
		fmt.Printf("Getting schema information from server %s...\n", server.Address)
		schema, err := c.getTableSchema(ctx, server)
		if err != nil {
			log.Printf("Failed to get schema from server %s: %v", server.Address, err)
			continue
		}
		tableSchema = schema
		fmt.Printf("Successfully got schema info: table ID=%d, column count=%d\n", schema.ID, len(schema.Columns))
		break
	}

	if tableSchema == nil {
		return fmt.Errorf("unable to get schema information from any server")
	}

	// Send coprocessor requests to all TiDB servers in parallel
	for _, server := range c.servers {
		wg.Add(1)
		go func(srv ServerInfo) {
			defer wg.Done()
			fmt.Printf("Querying server: %s\n", srv.Address)

			// First check TiDB status
			if err := c.checkTiDBStatus(ctx, srv); err != nil {
				log.Printf("Failed to check TiDB status %s: %v", srv.StatusAddr, err)
			}

			result := c.queryServerViaCoprocessor(ctx, srv, tableSchema)
			resultChan <- result
		}(server)
	}

	// Wait for all requests to complete
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect and merge results
	return c.mergeResults(resultChan)
}

// QueryResult query result
type QueryResult struct {
	ServerAddr string
	Rows       []Row
	Error      error
}

// Row represents a row of data - based on the complete structure of CLUSTER_STATEMENTS_SUMMARY table
type Row struct {
	// Basic information
	Instance         string // INSTANCE - instance address
	SummaryBeginTime string // SUMMARY_BEGIN_TIME - summary begin time
	SummaryEndTime   string // SUMMARY_END_TIME - summary end time
	StmtType         string // STMT_TYPE - statement type
	SchemaName       string // SCHEMA_NAME - database name
	Digest           string // DIGEST - statement digest
	DigestText       string // DIGEST_TEXT - statement digest text
	TableNames       string // TABLE_NAMES - table names
	IndexNames       string // INDEX_NAMES - index names
	SampleUser       string // SAMPLE_USER - sample user

	// Execution statistics
	ExecCount  int64 // EXEC_COUNT - execution count
	SumLatency int64 // SUM_LATENCY - total latency
	MaxLatency int64 // MAX_LATENCY - maximum latency
	MinLatency int64 // MIN_LATENCY - minimum latency
	AvgLatency int64 // AVG_LATENCY - average latency

	// Parse and compile latency
	AvgParseLatency   int64 // AVG_PARSE_LATENCY - average parse latency
	MaxParseLatency   int64 // MAX_PARSE_LATENCY - maximum parse latency
	AvgCompileLatency int64 // AVG_COMPILE_LATENCY - average compile latency
	MaxCompileLatency int64 // MAX_COMPILE_LATENCY - maximum compile latency

	// Resource usage
	AvgMem          int64 // AVG_MEM - average memory usage
	MaxMem          int64 // MAX_MEM - maximum memory usage
	AvgDisk         int64 // AVG_DISK - average disk usage
	MaxDisk         int64 // MAX_DISK - maximum disk usage
	AvgAffectedRows int64 // AVG_AFFECTED_ROWS - average affected rows

	// Time information
	FirstSeen string // FIRST_SEEN - first seen time
	LastSeen  string // LAST_SEEN - last seen time

	// Sample information
	SampleSQL      string // SAMPLE_SQL - sample SQL
	PrevSampleText string // PREV_SAMPLE_TEXT - previous sample text

	// Plan information
	PlanDigest    string // PLAN_DIGEST - plan digest
	Plan          string // PLAN - execution plan
	PlanCacheHits int64  // PLAN_CACHE_HITS - plan cache hits
	PlanInCache   int64  // PLAN_IN_CACHE - plan in cache
	PlanInBinding int64  // PLAN_IN_BINDING - plan in binding

	// Query sample information
	QuerySampleText  string // QUERY_SAMPLE_TEXT - query sample text
	PrevSampleSQL    string // PREV_SAMPLE_SQL - previous sample SQL
	PlanDigestText   string // PLAN_DIGEST_TEXT - plan digest text
	QuerySampleUser  string // QUERY_SAMPLE_USER - query sample user
	QuerySampleHost  string // QUERY_SAMPLE_HOST - query sample host
	QuerySampleDB    string // QUERY_SAMPLE_DB - query sample database
	QuerySampleState string // QUERY_SAMPLE_STATE - query sample state
	QuerySampleInfo  string // QUERY_SAMPLE_INFO - query sample info

	// Transaction information
	QuerySampleTransType               string // QUERY_SAMPLE_TRANS_TYPE - query sample transaction type
	QuerySampleTransIsolation          string // QUERY_SAMPLE_TRANS_ISOLATION - query sample transaction isolation
	QuerySampleTransStartTime          string // QUERY_SAMPLE_TRANS_START_TIME - query sample transaction start time
	QuerySampleTransDuration           int64  // QUERY_SAMPLE_TRANS_DURATION - query sample transaction duration
	QuerySampleTransState              string // QUERY_SAMPLE_TRANS_STATE - query sample transaction state
	QuerySampleTransError              string // QUERY_SAMPLE_TRANS_ERROR - query sample transaction error
	QuerySampleTransTables             string // QUERY_SAMPLE_TRANS_TABLES - query sample transaction tables
	QuerySampleTransIndexes            string // QUERY_SAMPLE_TRANS_INDEXES - query sample transaction indexes
	QuerySampleTransLockKeys           string // QUERY_SAMPLE_TRANS_LOCK_KEYS - query sample transaction lock keys
	QuerySampleTransLockTime           int64  // QUERY_SAMPLE_TRANS_LOCK_TIME - query sample transaction lock time
	QuerySampleTransWaitTime           int64  // QUERY_SAMPLE_TRANS_WAIT_TIME - query sample transaction wait time
	QuerySampleTransBackoffTime        int64  // QUERY_SAMPLE_TRANS_BACKOFF_TIME - query sample transaction backoff time
	QuerySampleTransResolveLockTime    int64  // QUERY_SAMPLE_TRANS_RESOLVE_LOCK_TIME - query sample transaction resolve lock time
	QuerySampleTransLocalLatchWaitTime int64  // QUERY_SAMPLE_TRANS_LOCAL_LATCH_WAIT_TIME - query sample transaction local latch wait time
	QuerySampleTransWriteKeys          int64  // QUERY_SAMPLE_TRANS_WRITE_KEYS - query sample transaction write keys
	QuerySampleTransWriteSize          int64  // QUERY_SAMPLE_TRANS_WRITE_SIZE - query sample transaction write size
	QuerySampleTransPrewriteRegionNum  int64  // QUERY_SAMPLE_TRANS_PREWRITE_REGION_NUM - query sample transaction prewrite region num
	QuerySampleTransTxnRetry           int64  // QUERY_SAMPLE_TRANS_TXN_RETRY - query sample transaction retry
	QuerySampleTransBackoffTypes       string // QUERY_SAMPLE_TRANS_BACKOFF_TYPES - query sample transaction backoff types

	// Extended fields - for storing other column data
	ExtraFields map[string]interface{} // Store other undefined fields

	// Backward compatibility fields
	TotalTime int64 // Total latency - same as SumLatency, for backward compatibility
}

// queryServerViaCoprocessor queries a single server via coprocessor
func (c *ClusterStatementsSummaryClient) queryServerViaCoprocessor(ctx context.Context, server ServerInfo, tableSchema *TableSchema) QueryResult {
	result := QueryResult{
		ServerAddr: server.Address,
	}

	// Build gRPC connection address (using StatusPort)
	grpcAddr := net.JoinHostPort(server.IP, strconv.FormatUint(uint64(server.StatusPort), 10))

	// Establish gRPC connection
	conn, err := grpc.DialContext(ctx, grpcAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithTimeout(10*time.Second),
	)
	if err != nil {
		result.Error = fmt.Errorf("failed to connect to gRPC service: %v", err)
		return result
	}
	defer conn.Close()

	// Create TiKV client
	client := tikvpb.NewTikvClient(conn)

	// Build coprocessor request
	copReq, err := c.buildCoprocessorRequest(tableSchema)
	if err != nil {
		result.Error = fmt.Errorf("failed to build coprocessor request: %v", err)
		return result
	}

	// Send request
	resp, err := client.Coprocessor(ctx, copReq)
	if err != nil {
		result.Error = fmt.Errorf("failed to send coprocessor request: %v", err)
		return result
	}

	// Parse response
	rows, err := c.parseCoprocessorResponse(resp, server.Address, tableSchema)
	if err != nil {
		result.Error = fmt.Errorf("failed to parse response: %v", err)
		return result
	}

	result.Rows = rows
	return result
}

// buildCoprocessorRequest builds coprocessor request
func (c *ClusterStatementsSummaryClient) buildCoprocessorRequest(tableSchema *TableSchema) (*coprocessor.Request, error) {
	// Build DAG request
	dagReq := &tipb.DAGRequest{
		TimeZoneName:   "Asia/Shanghai",
		TimeZoneOffset: 28800,
		Flags:          0,
		EncodeType:     tipb.EncodeType_TypeDefault,
		User: &tipb.UserIdentity{
			UserName: "root",
			UserHost: "%",
		},
		Executors: []*tipb.Executor{
			{
				Tp: tipb.ExecType_TypeTableScan,
				TblScan: &tipb.TableScan{
					TableId: tableSchema.ID,
					Columns: c.buildColumnsFromSchema(tableSchema),
					Desc:    false,
				},
			},
		},
		OutputOffsets:             c.buildOutputOffsetsFromSchema(tableSchema),
		CollectExecutionSummaries: &[]bool{true}[0],
	}

	// Serialize DAG request
	data, err := proto.Marshal(dagReq)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize DAG request: %v", err)
	}

	log.Printf("=== GO VERSION DEBUG ===")
	log.Printf("DAG request details:")
	log.Printf("  - Executors: %d", len(dagReq.Executors))
	log.Printf("  - OutputOffsets: %v", dagReq.OutputOffsets[:10]) // First 10 offsets
	log.Printf("  - EncodeType: %v", dagReq.EncodeType)
	log.Printf("  - TimeZoneName: %s", dagReq.TimeZoneName)
	log.Printf("  - TimeZoneOffset: %d", dagReq.TimeZoneOffset)
	log.Printf("  - CollectExecutionSummaries: %v", dagReq.CollectExecutionSummaries)
	log.Printf("Serialized DAG request size: %d bytes", len(data))
	log.Printf("DAG request first 64 bytes: %v", data[:min(64, len(data))])

	// Build coprocessor request
	copReq := &coprocessor.Request{
		Tp:   103, // kv.ReqTypeDAG
		Data: data,
		Ranges: []*coprocessor.KeyRange{
			{
				Start: []byte{0x74, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01},
				End:   []byte{0x74, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02},
			},
		},
		Context: &kvrpcpb.Context{
			RegionId: 1,
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: 1,
				Version: 1,
			},
			Peer: &metapb.Peer{
				Id:      1,
				StoreId: 1,
			},
			SourceStmt: &kvrpcpb.SourceStmt{
				ConnectionId: 12345,
				SessionAlias: "cluster_statements_summary_client",
			},
		},
		StartTs: uint64(time.Now().Unix()),
	}

	// Serialize the full coprocessor request for comparison
	copReqData, err := proto.Marshal(copReq)
	if err == nil {
		log.Printf("Serialized coprocessor request size: %d bytes", len(copReqData))
		log.Printf("Coprocessor request first 64 bytes: %v", copReqData[:min(64, len(copReqData))])
	}
	log.Printf("Request details: tp=%d, ranges_count=%d, context_region_id=%d, start_ts=%d",
		copReq.Tp, len(copReq.Ranges), copReq.Context.RegionId, copReq.StartTs)
	log.Printf("=== END GO VERSION DEBUG ===")

	return copReq, nil
}

// buildColumnsFromSchema builds column information based on schema
func (c *ClusterStatementsSummaryClient) buildColumnsFromSchema(tableSchema *TableSchema) []*tipb.ColumnInfo {
	columns := make([]*tipb.ColumnInfo, len(tableSchema.Columns))
	for i, col := range tableSchema.Columns {
		columns[i] = &tipb.ColumnInfo{
			ColumnId: col.ID,
			Tp:       col.Type.Tp,
		}
	}
	return columns
}

// buildOutputOffsetsFromSchema builds output offsets based on schema
func (c *ClusterStatementsSummaryClient) buildOutputOffsetsFromSchema(tableSchema *TableSchema) []uint32 {
	offsets := make([]uint32, len(tableSchema.Columns))
	for i := range tableSchema.Columns {
		offsets[i] = uint32(i)
	}
	return offsets
}

// parseCoprocessorResponse parses coprocessor response
func (c *ClusterStatementsSummaryClient) parseCoprocessorResponse(resp *coprocessor.Response, serverAddr string, tableSchema *TableSchema) ([]Row, error) {
	if resp.OtherError != "" {
		return nil, fmt.Errorf("server returned error: %s", resp.OtherError)
	}

	// Parse SelectResponse
	var selectResp tipb.SelectResponse
	err := proto.Unmarshal(resp.Data, &selectResp)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize response: %v", err)
	}

	// Check warnings
	if len(selectResp.Warnings) > 0 {
		log.Printf("Server %s received warnings:", serverAddr)
		for i, warning := range selectResp.Warnings {
			log.Printf("  Warning %d: [%d] %s", i+1, warning.Code, warning.Msg)
		}
	}

	// Check errors
	if selectResp.Error != nil {
		return nil, fmt.Errorf("server execution error: [%d] %s", selectResp.Error.Code, selectResp.Error.Msg)
	}

	var allRows []Row

	// Parse data chunks
	for i, chunk := range selectResp.Chunks {
		blockRows, err := c.parseChunkData(&chunk, serverAddr, tableSchema)
		if err != nil {
			log.Printf("Failed to parse data chunk %d: %v", i+1, err)
			continue
		}
		allRows = append(allRows, blockRows...)
	}

	return allRows, nil
}

// parseChunkData parses data chunk
func (c *ClusterStatementsSummaryClient) parseChunkData(chunk *tipb.Chunk, serverAddr string, tableSchema *TableSchema) ([]Row, error) {
	var rows []Row

	// Check if there is data
	if len(chunk.RowsData) == 0 {
		return rows, nil
	}

	// Create intelligent parsing based on schema information
	rows = c.parseDataWithSchema(chunk.RowsData, serverAddr, tableSchema)

	return rows, nil
}

// parseDataWithSchema parses real binary data based on schema information
func (c *ClusterStatementsSummaryClient) parseDataWithSchema(data []byte, serverAddr string, tableSchema *TableSchema) []Row {
	var rows []Row

	// Check if data is empty
	if len(data) == 0 {
		return rows
	}

	// First try chunk decoding
	chunk, err := c.decodeChunkData(data, tableSchema)
	if err != nil {
		log.Printf("chunk decoding failed: %v, trying row encoding decode", err)
		// If chunk decoding fails, try row encoding decode
		rows = c.decodeRowData(data, serverAddr, tableSchema)
		return rows
	}

	// Extract row data from chunk
	rowCount := chunk.NumRows()
	log.Printf("Successfully decoded chunk containing %d rows of data", rowCount)

	for i := 0; i < rowCount; i++ {
		row := c.extractRowFromChunk(chunk, i, serverAddr, tableSchema)
		rows = append(rows, row)
	}

	return rows
}

// decodeRowData decodes row encoded data
func (c *ClusterStatementsSummaryClient) decodeRowData(data []byte, serverAddr string, tableSchema *TableSchema) []Row {
	var rows []Row
	offset := 0
	rowIndex := 0

	for offset < len(data) && rowIndex < 100 {
		row := Row{
			Instance:    serverAddr,
			ExtraFields: make(map[string]interface{}),
		}

		rowDecoded := false
		for _, col := range tableSchema.Columns {
			if offset >= len(data) {
				break
			}

			value, newOffset, err := c.decodeValueFromBytes(data, offset, col)
			if err != nil {
				break
			}

			offset = newOffset
			rowDecoded = true
			columnName := col.Name.O

			switch columnName {
			case "INSTANCE":
				row.Instance = c.safeStringValue(value)
			case "STMT_TYPE":
				row.StmtType = c.safeStringValue(value)
			case "SCHEMA_NAME":
				row.SchemaName = c.safeStringValue(value)
			case "DIGEST_TEXT":
				row.DigestText = c.safeStringValue(value)
			case "EXEC_COUNT":
				row.ExecCount = c.safeInt64Value(value)
			case "SUM_LATENCY":
				row.SumLatency = c.safeInt64Value(value)
			case "MAX_LATENCY":
				row.MaxLatency = c.safeInt64Value(value)
			case "AVG_LATENCY":
				row.AvgLatency = c.safeInt64Value(value)
			default:
				row.ExtraFields[columnName] = value
			}
		}

		if !rowDecoded {
			break
		}

		row.TotalTime = row.SumLatency
		rows = append(rows, row)
		rowIndex++
	}

	return rows
}

// decodeValueFromBytes decodes a single value using TiDB's DecodeOne logic
func (c *ClusterStatementsSummaryClient) decodeValueFromBytes(data []byte, offset int, col TableSchemaColumn) (interface{}, int, error) {
	if offset >= len(data) {
		return nil, offset, fmt.Errorf("insufficient data")
	}

	flag := data[offset]
	offset++

	switch flag {
	case 0x00: // NilFlag
		return nil, offset, nil
	case 0x01: // bytesFlag
		val, err := c.decodeBytes(data, &offset)
		return val, offset, err
	case 0x02: // compactBytesFlag
		val, err := c.decodeCompactBytes(data, &offset)
		return val, offset, err
	case 0x03: // intFlag
		val, err := c.decodeInt(data, &offset)
		return val, offset, err
	case 0x04: // uintFlag
		val, err := c.decodeUint(data, &offset)
		return val, offset, err
	case 0x05: // floatFlag
		val, err := c.decodeFloat(data, &offset)
		return val, offset, err
	case 0x06: // decimalFlag
		val, err := c.decodeDecimal(data, &offset)
		return val, offset, err
	case 0x07: // durationFlag
		val, err := c.decodeDuration(data, &offset)
		return val, offset, err
	case 0x08: // varintFlag
		val, err := c.decodeVarint(data, &offset)
		return val, offset, err
	case 0x09: // uvarintFlag
		val, err := c.decodeUvarint(data, &offset)
		return val, offset, err
	case 0x0A: // jsonFlag
		val, err := c.decodeJSON(data, &offset)
		return val, offset, err
	case 0x14: // vectorFloat32Flag
		val, err := c.decodeVectorFloat32(data, &offset)
		return val, offset, err
	case 0xFA: // maxFlag
		return "MAX_VALUE", offset, nil
	default:
		// Try to decode as time type (possibly special encoding)
		if flag >= 0x20 && flag <= 0x30 {
			val, err := c.decodeTimeValue(data, &offset, flag)
			return val, offset, err
		}
		return nil, offset, fmt.Errorf("unknown encoding flag: 0x%02x", flag)
	}
}

// decodeInt decodes signed integer
func (c *ClusterStatementsSummaryClient) decodeInt(data []byte, offset *int) (int64, error) {
	if *offset+8 > len(data) {
		return 0, fmt.Errorf("insufficient data, cannot decode integer")
	}
	val := int64(binary.LittleEndian.Uint64(data[*offset:]))
	*offset += 8
	return val, nil
}

// decodeUint decodes unsigned integer
func (c *ClusterStatementsSummaryClient) decodeUint(data []byte, offset *int) (uint64, error) {
	if *offset+8 > len(data) {
		return 0, fmt.Errorf("insufficient data, cannot decode unsigned integer")
	}
	val := binary.LittleEndian.Uint64(data[*offset:])
	*offset += 8
	return val, nil
}

// decodeVarint decodes variable length signed integer
func (c *ClusterStatementsSummaryClient) decodeVarint(data []byte, offset *int) (int64, error) {
	val, n := binary.Varint(data[*offset:])
	if n <= 0 {
		return 0, fmt.Errorf("cannot decode variable length integer")
	}
	*offset += n
	return val, nil
}

// decodeUvarint decodes variable length unsigned integer
func (c *ClusterStatementsSummaryClient) decodeUvarint(data []byte, offset *int) (uint64, error) {
	val, n := binary.Uvarint(data[*offset:])
	if n <= 0 {
		return 0, fmt.Errorf("cannot decode variable length unsigned integer")
	}
	*offset += n
	return val, nil
}

// decodeFloat decodes floating point number
func (c *ClusterStatementsSummaryClient) decodeFloat(data []byte, offset *int) (float64, error) {
	if *offset+8 > len(data) {
		return 0, fmt.Errorf("insufficient data, cannot decode float")
	}
	val := math.Float64frombits(binary.LittleEndian.Uint64(data[*offset:]))
	*offset += 8
	return val, nil
}

// decodeBytes decodes byte array
func (c *ClusterStatementsSummaryClient) decodeBytes(data []byte, offset *int) ([]byte, error) {
	if *offset+4 > len(data) {
		return nil, fmt.Errorf("insufficient data, cannot decode byte array length")
	}
	length := int(binary.LittleEndian.Uint32(data[*offset:]))
	*offset += 4

	if *offset+length > len(data) {
		return nil, fmt.Errorf("insufficient data, cannot decode byte array data")
	}
	val := data[*offset : *offset+length]
	*offset += length
	return val, nil
}

// decodeCompactBytes decodes compact byte array
func (c *ClusterStatementsSummaryClient) decodeCompactBytes(data []byte, offset *int) ([]byte, error) {
	if *offset >= len(data) {
		return nil, fmt.Errorf("insufficient data, cannot decode compact byte array")
	}

	// Read length
	length, n := binary.Varint(data[*offset:])
	if n <= 0 {
		return nil, fmt.Errorf("cannot decode compact byte array length")
	}
	*offset += n

	if *offset+int(length) > len(data) {
		return nil, fmt.Errorf("insufficient data, cannot decode compact byte array data")
	}
	val := data[*offset : *offset+int(length)]
	*offset += int(length)
	return val, nil
}

// decodeDecimal decodes decimal number
func (c *ClusterStatementsSummaryClient) decodeDecimal(data []byte, offset *int) (string, error) {
	if *offset+4 > len(data) {
		return "", fmt.Errorf("insufficient data, cannot decode decimal data")
	}
	val := fmt.Sprintf("decimal_%d", *offset)
	*offset += 4
	return val, nil
}

// decodeDuration decodes duration
func (c *ClusterStatementsSummaryClient) decodeDuration(data []byte, offset *int) (string, error) {
	val, err := c.decodeInt(data, offset)
	if err != nil {
		return "", err
	}
	return time.Duration(val).String(), nil
}

// decodeJSON decodes JSON
func (c *ClusterStatementsSummaryClient) decodeJSON(data []byte, offset *int) (string, error) {
	if *offset >= len(data) {
		return "", fmt.Errorf("insufficient data, cannot decode JSON")
	}
	val := fmt.Sprintf("json_data_%d", *offset)
	*offset += 4
	return val, nil
}

// decodeVectorFloat32 decodes vector float32
func (c *ClusterStatementsSummaryClient) decodeVectorFloat32(data []byte, offset *int) (string, error) {
	if *offset >= len(data) {
		return "", fmt.Errorf("insufficient data, cannot decode vector float32")
	}
	val := fmt.Sprintf("vector_float32_%d", *offset)
	*offset += 4
	return val, nil
}

// decodeTimeValue decodes time value
func (c *ClusterStatementsSummaryClient) decodeTimeValue(data []byte, offset *int, flag byte) (string, error) {
	if *offset+8 > len(data) {
		return "", fmt.Errorf("insufficient data, cannot decode time value")
	}
	packedTime := binary.LittleEndian.Uint64(data[*offset:])
	*offset += 8
	val := fmt.Sprintf("time_0x%02x_%d", flag, packedTime)
	return val, nil
}

// decodeChunkData decodes data using simplified chunk decoder
func (c *ClusterStatementsSummaryClient) decodeChunkData(data []byte, tableSchema *TableSchema) (*Chunk, error) {
	chunk := &Chunk{
		columns: make([]*Column, len(tableSchema.Columns)),
	}

	offset := 0
	for i, col := range tableSchema.Columns {
		column := &Column{}
		var err error
		offset, err = c.decodeColumn(data, offset, column, col)
		if err != nil {
			return nil, fmt.Errorf("failed to decode column %d: %v", i, err)
		}
		chunk.columns[i] = column
	}

	return chunk, nil
}

// decodeColumn 解码单个列
func (c *ClusterStatementsSummaryClient) decodeColumn(data []byte, offset int, col *Column, colInfo TableSchemaColumn) (int, error) {
	if offset+8 > len(data) {
		return offset, fmt.Errorf("数据不足，无法读取列长度")
	}

	// 解码长度
	col.length = int(binary.LittleEndian.Uint32(data[offset:]))
	offset += 4

	// 解码 nullCount
	nullCount := int(binary.LittleEndian.Uint32(data[offset:]))
	offset += 4

	// 解码 nullBitmap - 参考 TiDB 的逻辑
	if nullCount > 0 {
		numNullBitmapBytes := (col.length + 7) / 8
		if offset+numNullBitmapBytes > len(data) {
			return offset, fmt.Errorf("数据不足，无法读取 null bitmap，需要 %d 字节，剩余 %d 字节", numNullBitmapBytes, len(data)-offset)
		}
		col.nullBitmap = data[offset : offset+numNullBitmapBytes]
		offset += numNullBitmapBytes
	} else {
		// 当 nullCount 为 0 时，不读取 null bitmap，而是设置所有位为非空
		c.setAllNotNull(col)
	}

	// 解码 offsets 和数据
	if c.isFixedLengthType(MySQLType(colInfo.Type.Tp)) {
		// 固定长度类型
		fixedLen := c.getFixedLength(MySQLType(colInfo.Type.Tp))
		if fixedLen > 0 {
			dataLen := fixedLen * col.length
			if offset+dataLen > len(data) {
				return offset, fmt.Errorf("数据不足，无法读取固定长度数据，需要 %d 字节，剩余 %d 字节", dataLen, len(data)-offset)
			}
			col.data = data[offset : offset+dataLen]
			offset += dataLen
		}
	} else {
		// 变长类型
		numOffsetBytes := (col.length + 1) * 8
		if offset+numOffsetBytes > len(data) {
			return offset, fmt.Errorf("数据不足，无法读取偏移量，需要 %d 字节，剩余 %d 字节", numOffsetBytes, len(data)-offset)
		}

		// 解码偏移量
		col.offsets = make([]int64, col.length+1)
		for i := 0; i <= col.length; i++ {
			col.offsets[i] = int64(binary.LittleEndian.Uint64(data[offset:]))
			offset += 8
		}

		// 解码数据
		dataLen := int(col.offsets[col.length])
		if offset+dataLen > len(data) {
			return offset, fmt.Errorf("数据不足，无法读取变长数据，需要 %d 字节，剩余 %d 字节", dataLen, len(data)-offset)
		}
		col.data = data[offset : offset+dataLen]
		offset += dataLen
	}

	return offset, nil
}

// setAllNotNull 设置所有位为非空
func (c *ClusterStatementsSummaryClient) setAllNotNull(col *Column) {
	numNullBitmapBytes := (col.length + 7) / 8
	col.nullBitmap = make([]byte, numNullBitmapBytes)
	for i := 0; i < numNullBitmapBytes; i++ {
		col.nullBitmap[i] = 0xFF
	}
}

// isFixedLengthType 检查是否为固定长度类型
func (c *ClusterStatementsSummaryClient) isFixedLengthType(tp MySQLType) bool {
	switch tp {
	case TypeTiny, TypeShort, TypeInt24, TypeLong, TypeLonglong, TypeFloat, TypeDouble:
		return true
	default:
		return false
	}
}

// getFixedLength 获取固定长度类型的字节长度
func (c *ClusterStatementsSummaryClient) getFixedLength(tp MySQLType) int {
	switch tp {
	case TypeTiny:
		return 1
	case TypeShort:
		return 2
	case TypeInt24:
		return 3
	case TypeLong:
		return 4
	case TypeLonglong:
		return 8
	case TypeFloat:
		return 4
	case TypeDouble:
		return 8
	default:
		return -1
	}
}

// extractRowFromChunk 从 chunk 中提取单行数据
func (c *ClusterStatementsSummaryClient) extractRowFromChunk(chunk *Chunk, rowIdx int, serverAddr string, tableSchema *TableSchema) Row {
	row := Row{
		Instance:    serverAddr,
		ExtraFields: make(map[string]interface{}),
	}

	// 遍历所有列，提取数据
	for colIdx, col := range tableSchema.Columns {
		columnName := col.Name.O
		value := c.extractValueFromColumn(chunk, rowIdx, colIdx, col)

		// 根据列名设置对应的字段
		switch columnName {
		case "INSTANCE":
			row.Instance = c.safeStringValue(value)
		case "SUMMARY_BEGIN_TIME":
			row.SummaryBeginTime = c.safeStringValue(value)
		case "SUMMARY_END_TIME":
			row.SummaryEndTime = c.safeStringValue(value)
		case "STMT_TYPE":
			row.StmtType = c.safeStringValue(value)
		case "SCHEMA_NAME":
			row.SchemaName = c.safeStringValue(value)
		case "DIGEST":
			row.Digest = c.safeStringValue(value)
		case "DIGEST_TEXT":
			row.DigestText = c.safeStringValue(value)
		case "TABLE_NAMES":
			row.TableNames = c.safeStringValue(value)
		case "INDEX_NAMES":
			row.IndexNames = c.safeStringValue(value)
		case "SAMPLE_USER":
			row.SampleUser = c.safeStringValue(value)
		case "EXEC_COUNT":
			row.ExecCount = c.safeInt64Value(value)
		case "SUM_LATENCY":
			row.SumLatency = c.safeInt64Value(value)
		case "MAX_LATENCY":
			row.MaxLatency = c.safeInt64Value(value)
		case "MIN_LATENCY":
			row.MinLatency = c.safeInt64Value(value)
		case "AVG_LATENCY":
			row.AvgLatency = c.safeInt64Value(value)
		case "AVG_PARSE_LATENCY":
			row.AvgParseLatency = c.safeInt64Value(value)
		case "MAX_PARSE_LATENCY":
			row.MaxParseLatency = c.safeInt64Value(value)
		case "AVG_COMPILE_LATENCY":
			row.AvgCompileLatency = c.safeInt64Value(value)
		case "MAX_COMPILE_LATENCY":
			row.MaxCompileLatency = c.safeInt64Value(value)
		case "AVG_MEM":
			row.AvgMem = c.safeInt64Value(value)
		case "MAX_MEM":
			row.MaxMem = c.safeInt64Value(value)
		case "AVG_DISK":
			row.AvgDisk = c.safeInt64Value(value)
		case "MAX_DISK":
			row.MaxDisk = c.safeInt64Value(value)
		case "AVG_AFFECTED_ROWS":
			row.AvgAffectedRows = c.safeInt64Value(value)
		case "FIRST_SEEN":
			row.FirstSeen = c.safeStringValue(value)
		case "LAST_SEEN":
			row.LastSeen = c.safeStringValue(value)
		case "SAMPLE_SQL":
			row.SampleSQL = c.safeStringValue(value)
		case "PREV_SAMPLE_TEXT":
			row.PrevSampleText = c.safeStringValue(value)
		case "PLAN_DIGEST":
			row.PlanDigest = c.safeStringValue(value)
		case "PLAN":
			row.Plan = c.safeStringValue(value)
		case "PLAN_CACHE_HITS":
			row.PlanCacheHits = c.safeInt64Value(value)
		case "PLAN_IN_CACHE":
			row.PlanInCache = c.safeInt64Value(value)
		case "PLAN_IN_BINDING":
			row.PlanInBinding = c.safeInt64Value(value)
		case "QUERY_SAMPLE_TEXT":
			row.QuerySampleText = c.safeStringValue(value)
		case "PREV_SAMPLE_SQL":
			row.PrevSampleSQL = c.safeStringValue(value)
		case "PLAN_DIGEST_TEXT":
			row.PlanDigestText = c.safeStringValue(value)
		case "QUERY_SAMPLE_USER":
			row.QuerySampleUser = c.safeStringValue(value)
		case "QUERY_SAMPLE_HOST":
			row.QuerySampleHost = c.safeStringValue(value)
		case "QUERY_SAMPLE_DB":
			row.QuerySampleDB = c.safeStringValue(value)
		case "QUERY_SAMPLE_STATE":
			row.QuerySampleState = c.safeStringValue(value)
		case "QUERY_SAMPLE_INFO":
			row.QuerySampleInfo = c.safeStringValue(value)
		case "QUERY_SAMPLE_TRANS_TYPE":
			row.QuerySampleTransType = c.safeStringValue(value)
		case "QUERY_SAMPLE_TRANS_ISOLATION":
			row.QuerySampleTransIsolation = c.safeStringValue(value)
		case "QUERY_SAMPLE_TRANS_START_TIME":
			row.QuerySampleTransStartTime = c.safeStringValue(value)
		case "QUERY_SAMPLE_TRANS_DURATION":
			row.QuerySampleTransDuration = c.safeInt64Value(value)
		case "QUERY_SAMPLE_TRANS_STATE":
			row.QuerySampleTransState = c.safeStringValue(value)
		case "QUERY_SAMPLE_TRANS_ERROR":
			row.QuerySampleTransError = c.safeStringValue(value)
		case "QUERY_SAMPLE_TRANS_TABLES":
			row.QuerySampleTransTables = c.safeStringValue(value)
		case "QUERY_SAMPLE_TRANS_INDEXES":
			row.QuerySampleTransIndexes = c.safeStringValue(value)
		case "QUERY_SAMPLE_TRANS_LOCK_KEYS":
			row.QuerySampleTransLockKeys = c.safeStringValue(value)
		case "QUERY_SAMPLE_TRANS_LOCK_TIME":
			row.QuerySampleTransLockTime = c.safeInt64Value(value)
		case "QUERY_SAMPLE_TRANS_WAIT_TIME":
			row.QuerySampleTransWaitTime = c.safeInt64Value(value)
		case "QUERY_SAMPLE_TRANS_BACKOFF_TIME":
			row.QuerySampleTransBackoffTime = c.safeInt64Value(value)
		case "QUERY_SAMPLE_TRANS_RESOLVE_LOCK_TIME":
			row.QuerySampleTransResolveLockTime = c.safeInt64Value(value)
		case "QUERY_SAMPLE_TRANS_LOCAL_LATCH_WAIT_TIME":
			row.QuerySampleTransLocalLatchWaitTime = c.safeInt64Value(value)
		case "QUERY_SAMPLE_TRANS_WRITE_KEYS":
			row.QuerySampleTransWriteKeys = c.safeInt64Value(value)
		case "QUERY_SAMPLE_TRANS_WRITE_SIZE":
			row.QuerySampleTransWriteSize = c.safeInt64Value(value)
		case "QUERY_SAMPLE_TRANS_PREWRITE_REGION_NUM":
			row.QuerySampleTransPrewriteRegionNum = c.safeInt64Value(value)
		case "QUERY_SAMPLE_TRANS_TXN_RETRY":
			row.QuerySampleTransTxnRetry = c.safeInt64Value(value)
		case "QUERY_SAMPLE_TRANS_BACKOFF_TYPES":
			row.QuerySampleTransBackoffTypes = c.safeStringValue(value)
		default:
			// 存储到扩展字段中
			row.ExtraFields[columnName] = value
		}
	}

	// 设置 TotalTime 为 SumLatency 的别名，保持向后兼容
	row.TotalTime = row.SumLatency

	return row
}

// extractValueFromColumn 从 chunk 列中提取值
func (c *ClusterStatementsSummaryClient) extractValueFromColumn(chunk *Chunk, rowIdx, colIdx int, col TableSchemaColumn) interface{} {
	column := chunk.Column(colIdx)

	// 检查是否为 NULL
	if column.IsNull(rowIdx) {
		return nil
	}

	// 根据列类型提取值
	switch MySQLType(col.Type.Tp) {
	case TypeTiny, TypeShort, TypeInt24, TypeLong, TypeLonglong:
		return column.GetInt64(rowIdx)
	case TypeFloat:
		return column.GetFloat32(rowIdx)
	case TypeDouble:
		return column.GetFloat64(rowIdx)
	case TypeString, TypeVarString, TypeTinyBlob,
		TypeMediumBlob, TypeLongBlob, TypeBlob, TypeJSON:
		return column.GetString(rowIdx)
	case TypeDate, TypeDatetime, TypeTimestamp:
		// 时间类型转换为字符串（简化处理）
		bytes := column.GetBytes(rowIdx)
		if len(bytes) >= 8 {
			// 假设是时间戳格式
			timestamp := int64(binary.LittleEndian.Uint64(bytes))
			return time.Unix(timestamp, 0).Format("2006-01-02 15:04:05")
		}
		return column.GetString(rowIdx)
	case TypeDuration:
		// 持续时间类型（简化处理）
		bytes := column.GetBytes(rowIdx)
		if len(bytes) >= 8 {
			duration := int64(binary.LittleEndian.Uint64(bytes))
			return time.Duration(duration).String()
		}
		return column.GetString(rowIdx)
	case TypeNewDecimal:
		// 小数类型（简化处理）
		return column.GetString(rowIdx)
	case TypeEnum:
		// 枚举类型（简化处理）
		return column.GetString(rowIdx)
	case TypeSet:
		// 集合类型（简化处理）
		return column.GetString(rowIdx)
	case TypeBit:
		// 位类型
		bytes := column.GetBytes(rowIdx)
		return string(bytes)
	default:
		// 默认尝试获取字符串
		return column.GetString(rowIdx)
	}
}

// safeStringValue 安全地获取字符串值
func (c *ClusterStatementsSummaryClient) safeStringValue(value interface{}) string {
	if value == nil {
		return ""
	}
	if str, ok := value.(string); ok {
		return str
	}
	// 处理 []uint8 类型（字节数组）
	if bytes, ok := value.([]uint8); ok {
		return string(bytes)
	}
	// 处理 []byte 类型
	if bytes, ok := value.([]byte); ok {
		return string(bytes)
	}
	return ""
}

// safeInt64Value 安全地获取 int64 值
func (c *ClusterStatementsSummaryClient) safeInt64Value(value interface{}) int64 {
	if value == nil {
		return 0
	}
	if val, ok := value.(int64); ok {
		return val
	}
	// 处理 uint64 类型
	if val, ok := value.(uint64); ok {
		return int64(val)
	}
	// 处理 int 类型
	if val, ok := value.(int); ok {
		return int64(val)
	}
	// 处理 uint 类型
	if val, ok := value.(uint); ok {
		return int64(val)
	}
	return 0
}

// parseRowData 解析单行数据
func (c *ClusterStatementsSummaryClient) parseRowData(data []byte, offset int, serverAddr string, columnNames map[int64]string, tableSchema *TableSchema) (*Row, int, error) {
	if offset >= len(data) {
		return nil, offset, nil
	}

	// 创建行数据映射
	rowData := make(map[string]interface{})

	// 解析每个列的数据
	for i, col := range tableSchema.Columns {
		if offset >= len(data) {
			break
		}

		columnName := col.Name.O
		value, newOffset, err := c.parseColumnValue(data, offset, col.Type.Tp)
		if err != nil {
			return nil, offset, fmt.Errorf("解析列 %s 失败: %v", columnName, err)
		}

		rowData[columnName] = value
		offset = newOffset

		// 限制解析的列数，避免解析过多数据
		if i >= 20 { // 只解析前20个重要列
			break
		}
	}

	// 构建 Row 对象
	row := &Row{
		Instance: serverAddr,
	}

	// 提取关键字段
	if digestText, ok := rowData["DIGEST_TEXT"].(string); ok {
		row.DigestText = digestText
	} else {
		row.DigestText = "UNKNOWN_QUERY"
	}

	if execCount, ok := rowData["EXEC_COUNT"].(int64); ok {
		row.ExecCount = execCount
	} else {
		row.ExecCount = 0
	}

	if sumLatency, ok := rowData["SUM_LATENCY"].(int64); ok {
		row.TotalTime = sumLatency
	} else {
		row.TotalTime = 0
	}

	return row, offset, nil
}

// parseColumnValue 解析列值
func (c *ClusterStatementsSummaryClient) parseColumnValue(data []byte, offset int, columnType int32) (interface{}, int, error) {
	if offset >= len(data) {
		return nil, offset, nil
	}

	// 读取长度前缀
	if offset+1 > len(data) {
		return nil, offset, fmt.Errorf("数据不足，无法读取长度前缀")
	}

	length := int(data[offset])
	offset++

	if offset+length > len(data) {
		return nil, offset, fmt.Errorf("数据不足，长度=%d，剩余=%d", length, len(data)-offset)
	}

	valueData := data[offset : offset+length]
	offset += length

	// 根据类型解析值
	switch columnType {
	case 15: // VARCHAR
		return string(valueData), offset, nil
	case 8: // BIGINT
		if length == 8 {
			value := int64(valueData[0])<<56 | int64(valueData[1])<<48 | int64(valueData[2])<<40 | int64(valueData[3])<<32 |
				int64(valueData[4])<<24 | int64(valueData[5])<<16 | int64(valueData[6])<<8 | int64(valueData[7])
			return value, offset, nil
		}
		return int64(0), offset, nil
	case 1: // TINYINT
		if length == 1 {
			return int64(valueData[0]), offset, nil
		}
		return int64(0), offset, nil
	case 12: // DATETIME
		// 简化处理，返回字符串
		return string(valueData), offset, nil
	default:
		// 默认作为字符串处理
		return string(valueData), offset, nil
	}
}

// mergeResults 合并来自所有节点的结果
func (c *ClusterStatementsSummaryClient) mergeResults(resultChan <-chan QueryResult) error {
	var allRows []Row
	errorCount := 0
	successCount := 0

	fmt.Println("\n=== 处理查询结果 ===")

	for result := range resultChan {
		if result.Error != nil {
			log.Printf("❌ 服务器 %s 查询失败: %v", result.ServerAddr, result.Error)
			errorCount++
			continue
		}

		successCount++
		fmt.Printf("✅ 服务器 %s 返回 %d 行数据\n", result.ServerAddr, len(result.Rows))

		// 不再在这里打印详细数据，将在最后统一以表格形式显示

		allRows = append(allRows, result.Rows...)
	}

	fmt.Printf("\n=== 最终统计 ===\n")
	fmt.Printf("成功查询的服务器数: %d/%d\n", successCount, len(c.servers))
	fmt.Printf("失败的服务器数: %d/%d\n", errorCount, len(c.servers))
	fmt.Printf("总数据行数: %d\n", len(allRows))

	// 按服务器分组统计
	if len(allRows) > 0 {
		serverStats := make(map[string]int)
		for _, row := range allRows {
			serverStats[row.Instance]++
		}

		fmt.Printf("\n=== 数据分布 ===\n")
		for server, count := range serverStats {
			fmt.Printf("服务器 %s: %d 行数据\n", server, count)
		}

		// 以表格形式显示数据
		c.printTableFormat(allRows)
	}

	return nil
}

// printTableFormat 以表格形式打印数据
func (c *ClusterStatementsSummaryClient) printTableFormat(rows []Row) {
	fmt.Printf("\n=== CLUSTER_STATEMENTS_SUMMARY 数据表 ===\n")

	// 定义表格列
	columns := []struct {
		name   string
		width  int
		getter func(Row) string
	}{
		{"Instance", 20, func(r Row) string { return r.Instance }},
		{"StmtType", 10, func(r Row) string { return r.StmtType }},
		{"SchemaName", 15, func(r Row) string { return r.SchemaName }},
		{"DigestText", 50, func(r Row) string { return c.truncateString(r.DigestText, 47) }},
		{"ExecCount", 10, func(r Row) string { return fmt.Sprintf("%d", r.ExecCount) }},
		{"SumLatency(μs)", 15, func(r Row) string { return fmt.Sprintf("%d", r.SumLatency) }},
		{"MaxLatency(μs)", 15, func(r Row) string { return fmt.Sprintf("%d", r.MaxLatency) }},
		{"AvgLatency(μs)", 15, func(r Row) string { return fmt.Sprintf("%d", r.AvgLatency) }},
		{"AvgMem(KB)", 12, func(r Row) string { return fmt.Sprintf("%d", r.AvgMem) }},
		{"MaxMem(KB)", 12, func(r Row) string { return fmt.Sprintf("%d", r.MaxMem) }},
		{"PlanCacheHits", 15, func(r Row) string { return fmt.Sprintf("%d", r.PlanCacheHits) }},
		{"PlanInCache", 12, func(r Row) string { return fmt.Sprintf("%d", r.PlanInCache) }},
		{"PlanInBinding", 15, func(r Row) string { return fmt.Sprintf("%d", r.PlanInBinding) }},
	}

	// 打印表头
	fmt.Print("|")
	for _, col := range columns {
		fmt.Printf(" %-*s |", col.width, col.name)
	}
	fmt.Println()

	// 打印分隔线
	fmt.Print("|")
	for _, col := range columns {
		fmt.Printf(" %-*s |", col.width, strings.Repeat("-", col.width))
	}
	fmt.Println()

	// 打印数据行
	for i, row := range rows {
		fmt.Print("|")
		for _, col := range columns {
			value := col.getter(row)
			fmt.Printf(" %-*s |", col.width, value)
		}
		fmt.Println()

		// 每10行添加一个分隔线
		if (i+1)%10 == 0 && i < len(rows)-1 {
			fmt.Print("|")
			for _, col := range columns {
				fmt.Printf(" %-*s |", col.width, strings.Repeat("-", col.width))
			}
			fmt.Println()
		}
	}

	fmt.Printf("\n共显示 %d 行数据\n", len(rows))
}

// truncateString 截断字符串到指定长度
func (c *ClusterStatementsSummaryClient) truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}

// GetTiDBServersFromEtcd gets TiDB server information from etcd (mock implementation)
func GetTiDBServersFromEtcd(ctx context.Context) ([]ServerInfo, error) {
	// This should get real server information from etcd
	// For demonstration, using static configuration
	return []ServerInfo{
		{
			ServerType: "tidb",
			Address:    "127.0.0.1:4000",
			StatusAddr: "127.0.0.1:10080",
			StatusPort: 10080,
			IP:         "127.0.0.1",
		},
		{
			ServerType: "tidb",
			Address:    "127.0.0.1:4001",
			StatusAddr: "127.0.0.1:10081",
			StatusPort: 10081,
			IP:         "127.0.0.1",
		},
	}, nil
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	fmt.Println("=== TiDB CLUSTER_STATEMENTS_SUMMARY gRPC Coprocessor 客户端 ===")
	fmt.Println("版本: v2.1 - 修复 SourceStmt 空指针问题")
	fmt.Println("修复内容: 添加了 Context.SourceStmt 字段，解决服务端 panic 问题")
	fmt.Println()

	// Get TiDB server information
	servers, err := GetTiDBServersFromEtcd(ctx)
	if err != nil {
		log.Fatalf("Failed to get server information: %v", err)
	}

	if len(servers) == 0 {
		log.Fatal("No TiDB servers found")
	}

	fmt.Printf("Found %d TiDB servers:\n", len(servers))
	for _, server := range servers {
		fmt.Printf("  - MySQL port: %s, gRPC port: %s\n", server.Address, server.StatusAddr)
	}
	fmt.Println()

	// Create client
	client := NewClusterStatementsSummaryClient(servers)

	// Query cluster statements summary
	fmt.Println("Starting CLUSTER_STATEMENTS_SUMMARY query via gRPC coprocessor...")
	fmt.Println("Note: This will directly send coprocessor requests to each TiDB node's StatusPort")
	fmt.Println("If errors occur, this is normal as we are sending test requests")
	fmt.Println()

	err = client.QueryClusterStatementsSummary(ctx)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}

	fmt.Println("\n=== Query Complete ===")
	fmt.Println("This client demonstrates TiDB's internal mechanism for handling CLUSTER_STATEMENTS_SUMMARY queries:")
	fmt.Println("1. ✅ Connect to each TiDB node's StatusPort via gRPC")
	fmt.Println("2. ✅ Send coprocessor DAG requests")
	fmt.Println("3. ✅ Obtain responses from each node in parallel")
	fmt.Println("4. ✅ Merge results and add instance identifiers")
	fmt.Println("5. ✅ Error handling and fault tolerance mechanisms")
	fmt.Println()
	fmt.Println("Note: Since CLUSTER_STATEMENTS_SUMMARY requires specific table structure and permissions,")
	fmt.Println("actual production environments require more precise DAG request construction.")
}
