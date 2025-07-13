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

1. **list_databases** - List all available databases
   - No parameters required
   - Shows all registered databases with descriptions

2. **get_schema_tool** - Get database schema information
   - Required parameter: `database` - Database name to examine
   - Example: `get_schema_tool(database="sales_db")`

3. **query_data** - Execute SQL queries across multiple databases
   - Required parameter: `sql` - SQL query using [database].[table] syntax
   - Example: `query_data(sql="SELECT TOP 10 * FROM [sales_db].[Customers]")`

4. **test_cross_db_connectivity** - Test database connections and cross-DB functionality
   - No parameters required
   - Helps diagnose connection issues

5. **query_builder_help** - Get examples for building Access SQL queries
   - Required parameter: `query_type` - Type of query help needed
   - Options: simple_select, cross_database_join, union, aggregation, date_filtering, boolean_filtering
   - Example: `query_builder_help(query_type="cross_database_join")`

6. **validate_query_syntax** - Validate Access SQL syntax before execution
   - Required parameter: `sql` - Query to validate
   - Checks for common Access SQL mistakes

7. **query_limitations** - Learn about Access limitations and workarounds
   - Required parameter: `topic` - Limitation topic
   - Options: joins, performance, data_types, functions, best_practices, all
   - Example: `query_limitations(topic="joins")`

## Cross-Database Query Support

This tool supports queries across multiple Access databases using a special syntax:

```sql
-- Always use [database_name].[table_name] format
SELECT [c].[CustomerName], [o].[OrderTotal]
FROM [sales_db].[Customers] AS [c], [orders_db].[Orders] AS [o]
WHERE [c].[CustomerID] = [o].[CustomerID]
AND [o].[OrderDate] > #2024-01-01#
```

### Important Access SQL Rules

- **Dates**: Use `#YYYY-MM-DD#` format (not quotes)
- **Booleans**: Use `1/0` instead of `True/False`
- **String concatenation**: Use `&` instead of `+`
- **Limit results**: Use `TOP N` instead of `LIMIT`
- **Conditionals**: Use `IIF()` instead of `CASE WHEN`

## Known Limitations

### Cross-Database JOINs
Direct JOINs between databases often fail. Use these workarounds:
- WHERE clause filtering: `FROM [db1].[t1], [db2].[t2] WHERE [t1].[id]=[t2].[id]`
- UNION operations to combine results
- Query databases separately and combine in application code

### Performance
- Database files limited to 2GB
- Queries slow down significantly with >100k records
- Always use `TOP N` or `WHERE` clauses to limit results

### Data Types
- Boolean fields may use 1/0 or -1/0
- Text fields limited to 255 characters (use Memo for larger)
- Use conversion functions: `CInt()`, `CDbl()`, `CStr()`, `CDate()`

### Best Practices
1. Always use `[database].[table]` syntax for all tables
2. Test with small datasets first
3. Use `validate_query_syntax` before executing complex queries
4. Run `query_limitations` tool to understand constraints
5. Keep related data in the same database when possible

## Troubleshooting

If you encounter errors:
1. Run `test_cross_db_connectivity` to check all databases are accessible
2. Use `get_schema_tool` to verify table and column names
3. Check `query_builder_help` for correct syntax examples
4. Review `query_limitations` for known issues and workarounds

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
