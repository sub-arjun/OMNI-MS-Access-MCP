# MS Access MCP Explorer

A tool for exploring and querying Microsoft Access databases using MCP (Model-Command-Procedure).

## Installation

Install using UV:

```
uv pip install omni-ms-access-mcp
```

Or using pip:

```
pip install omni-ms-access-mcp
```

## Usage

### Command Line

Start the MCP server with the path to your Access database:

```
ms-access-mcp --db-path "C:\path\to\your\database.accdb"
```

### Python API

```python
from omni_ms_access_mcp.server import AccessMCP

# Initialize the MCP server
mcp_server = AccessMCP(db_path="C:/path/to/your/database.accdb")

# Run the server
mcp_server.run()
```

## Available Tools

1. **get_schema_tool** - Get database schema information
   - Optional parameter: `format` - Can be "text" (default) or "json"
   - Example: `get_schema_tool(format="json")`

2. **query_data** - Execute SQL queries against the database
   - Required parameter: `sql` - SQL query to execute
   - Example: `query_data(sql="SELECT * FROM Users LIMIT 10")`

## Development

### Setup Development Environment

```
uv venv
.venv\Scripts\activate.bat
uv pip install -e .
```

### Building the Package

```
publish.bat
```

### Publishing to Package Index

```
uv pip install -U twine
uv python -m twine upload dist/*
```

## Requirements

- Microsoft Access Driver must be installed on your system
- Python 3.7 or higher
