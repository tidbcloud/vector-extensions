# Vector Extensions Demo

Data synchronization system demo - Control Vector via API to perform slowlog backup tasks from S3 to MySQL.

## Quick Start

```bash
# 1. Initialize environment
./scripts/01_setup.sh

# 2. Start server
./scripts/02_start.sh

# 3. Run tests (in another terminal)
./scripts/03_test.sh
```

## Documentation

Detailed documentation is available in the `doc/v1/` directory:

- [User Guide](../doc/v1/readme.md) - Complete usage instructions and API documentation
- [Architecture Documentation](../doc/v1/arch.md) - System architecture and design
- [AI Agent Guide](../doc/v1/agent.md) - Development guide

## Project Structure

```
demo/
├── app.py                    # Flask API server
├── requirements.txt          # Python dependencies
├── scripts/                  # Scripts directory
│   ├── 01_setup.sh          # Initialize environment
│   ├── 02_start.sh          # Start server
│   ├── 03_test.sh           # End-to-end test
│   └── 04_test_api.sh       # API test
├── config/                   # Configuration files
│   ├── create_mysql_table.sql
│   └── test_request.json
└── tests/                    # Test scripts
    ├── run_full_test.py
    └── direct_import.py
```

## Prerequisites

- Python 3.8+
- Vector binary (auto-detected at `target/debug/vector` or `target/release/vector`)
- MySQL (local or Docker)
- AWS credentials (for accessing S3)

## More Information

See [doc/v1/readme.md](../doc/v1/readme.md) for complete documentation.
