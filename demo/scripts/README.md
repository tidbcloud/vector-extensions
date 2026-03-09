# Script Usage Guide

## Script List

### 01_setup.sh - Initialize Environment

**Functions**:
- Create MySQL database and tables
- Prompt for AWS credentials configuration

**Usage**:
```bash
./scripts/01_setup.sh
```

**Notes**:
- Automatically detects MySQL Docker container
- If container not found, prompts for manual creation
- Prompts for AWS credentials configuration (via environment variables)

### 02_start.sh - Start Server

**Functions**:
- Check and install Python dependencies
- Check MySQL connection
- Auto-detect Vector binary
- Start Flask API server

**Usage**:
```bash
./scripts/02_start.sh
```

**Notes**:
- Server will start at `http://0.0.0.0:8080`
- Automatically detects Vector binary (`target/debug/vector` or `target/release/vector`)
- If Vector not found, system automatically falls back to direct import mode

### 03_test.sh - End-to-End Test

**Functions**:
- Health check
- Create backup task
- Query task status
- Check MySQL data

**Usage**:
```bash
./scripts/03_test.sh
```

**Prerequisites**:
- Server is running (run `02_start.sh`)
- MySQL is configured (run `01_setup.sh`)

### 04_test_api.sh - API Test

**Functions**:
- Test various API endpoints

**Usage**:
```bash
./scripts/04_test_api.sh
```

## Usage Order

```bash
# 1. Initialize environment
./scripts/01_setup.sh

# 2. Start server (in one terminal)
./scripts/02_start.sh

# 3. Run tests (in another terminal)
./scripts/03_test.sh
```

## Notes

1. **Script Path**: All scripts use relative paths, recommended to run from `demo/` directory
2. **Permissions**: Ensure scripts have execute permissions (`chmod +x scripts/*.sh`)
3. **Environment Variables**: Some scripts require environment variables (e.g., AWS credentials)
