import pyodbc
import os
import json
import argparse
import sys
import asyncio
from typing import Any, Sequence

from mcp.server.models import InitializationOptions
import mcp.types as types
from mcp.server import NotificationOptions, Server
from mcp.server.stdio import stdio_server
from mcp.server.sse import SseServerTransport
from starlette.applications import Starlette
from starlette.routing import Mount, Route
import uvicorn
import re

# Parse command-line arguments
parser = argparse.ArgumentParser(description="MS Access Explorer MCP Server")
parser.add_argument("--db-path", type=str, action='append', dest='db_paths', 
                   help="Path to Access database file (.accdb or .mdb). Use multiple times for multiple databases.")
parser.add_argument("--db-name", type=str, action='append', dest='db_names',
                   help="Friendly name for the database. Must match the order of --db-path arguments.")
parser.add_argument("--db-desc", type=str, action='append', dest='db_descriptions',
                   help="Description for the database. Must match the order of --db-path arguments.")
parser.add_argument('--transport', default='stdio', choices=['stdio', 'sse'], help='Transport method (default: stdio)')
parser.add_argument('--host', default='127.0.0.1', help='Host for SSE (default: 127.0.0.1)')
parser.add_argument('--port', type=int, default=8000, help='Port for SSE (default: 8000)')
parser.add_argument('--path', default='/sse', help='Path for SSE endpoint (default: /sse)')
args = parser.parse_args()

# Get the database paths
db_paths = args.db_paths or []
db_names = args.db_names or []
db_descriptions = args.db_descriptions or []

if not db_paths:
    print("Error: No database paths provided. Use --db-path argument.")
    print("Example: ms-access-mcp --db-path \"C:\\path\\to\\database1.accdb\" --db-path \"C:\\path\\to\\database2.accdb\"")
    print("With names: ms-access-mcp --db-path \"db1.accdb\" --db-name \"Sales\" --db-path \"db2.accdb\" --db-name \"Inventory\"")
    print("Full example: ms-access-mcp --db-path \"sales.accdb\" --db-name \"Sales\" --db-desc \"Sales and customer data\" --db-path \"inventory.accdb\" --db-name \"Inventory\" --db-desc \"Product inventory database\"")
    sys.exit(1)

# Create database registry
databases = {}
for i, db_path in enumerate(db_paths):
    # Verify the database exists
    if not os.path.exists(db_path):
        print(f"Error: Database file not found at {db_path}")
        sys.exit(1)
    
    # Use provided name or generate one
    if i < len(db_names) and db_names[i]:
        db_key = db_names[i]
    else:
        db_key = f"db_{i+1}"
    
    # Use provided description or default
    if i < len(db_descriptions) and db_descriptions[i]:
        db_desc = db_descriptions[i]
    else:
        db_desc = f"Database {db_key}"
    
    databases[db_key] = {
        'path': db_path,
        'description': db_desc
    }
    print(f"Registered database '{db_key}': {db_path}")
    print(f"  Description: {db_desc}")

# Set default database (first one)
default_db_key = list(databases.keys())[0]
print(f"Default database: {default_db_key}")

# Initialize the MCP server
server = Server("MS Access Explorer")

def get_database_path(db_name: str = None) -> tuple[str, str]:
    """Get database path and key from name
    
    Returns:
        tuple: (db_key, db_path)
    """
    if not db_name:
        db_key = default_db_key
        db_path = databases[default_db_key]['path']
    elif db_name in databases:
        db_key = db_name
        db_path = databases[db_name]['path']
    else:
        raise ValueError(f"Database '{db_name}' not found. Available databases: {list(databases.keys())}")
    
    return db_key, db_path

@server.list_resources()
async def handle_list_resources() -> list[types.Resource]:
    """List available resources"""
    return [
        types.Resource(
            uri=f"schema://{db_key}",
            name=f"Database Schema: {db_key}",
            description=f"Schema for {db_info['description']}",
            mimeType="text/plain",
        )
        for db_key, db_info in databases.items()
    ]

@server.read_resource()
async def handle_read_resource(uri: types.AnyUrl) -> str:
    """Read a resource"""
    if str(uri).startswith("schema://"):
        db_key = str(uri).replace("schema://", "")
        if db_key not in databases:
            raise ValueError(f"Database '{db_key}' not found")
        
        db_path = databases[db_key]['path']
        
        # Create a connection string
        conn_str = (
            r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
            fr'DBQ={db_path};'
        )
        
        # Establish the connection
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        # Get table schema information
        tables = [f"Database: {db_key} ({databases[db_key]['description']})"]
        for row in cursor.tables(tableType='TABLE'):
            if not row.table_name.startswith('MSys'):  # Skip system tables
                table_name = row.table_name
                tables.append(f"Table: {table_name}")
                # Get column information for each table
                columns = cursor.columns(table=table_name)
                for column in columns:
                    tables.append(f"  - Column: {column.column_name}, Type: {column.type_name}")
        
        conn.close()
        return "\n".join(tables)
    else:
        raise ValueError(f"Unknown resource: {uri}")

@server.list_tools()
async def handle_list_tools() -> list[types.Tool]:
    """List available tools"""
    return [
        types.Tool(
            name="list_databases",
            description="List all available databases. Use this when you need to know what databases exist or their descriptions. No parameters required.",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": [],
            },
        ),
        types.Tool(
            name="get_schema_tool",
            description="Get comprehensive database schema information for AI agents. Always specify the database name. Example: {'database': 'mrpplus_be', 'format': 'json', 'include_samples': true}",
            inputSchema={
                "type": "object",
                "properties": {
                    "database": {
                        "type": "string",
                        "description": "Name of the database to query (required for multi-DB awareness)",
                    },
                    "format": {
                        "type": "string",
                        "description": "Output format - 'text' or 'json'",
                        "enum": ["text", "json"],
                        "default": "text",
                    },
                    "include_samples": {
                        "type": "boolean",
                        "description": "Whether to include sample data for each table",
                        "default": False,
                    },
                },
                "required": ["database"],
            },
        ),
        types.Tool(
            name="query_data",
            description="Execute SQL queries across multiple databases. ALWAYS use [database_name].[table_name] syntax for ALL tables. The system automatically converts cross-database references to Access IN clause syntax. Use Access SQL dialect: square brackets around names, # for dates (#2023-01-01#), & for string concatenation, IIF() for conditionals, TOP N instead of LIMIT. Example: SELECT [c].[Name] FROM [sales_db].[Customers] AS [c] JOIN [orders_db].[Orders] AS [o] ON [c].[ID] = [o].[CustomerID] WHERE [o].[Date] > #2023-01-01# ORDER BY [c].[Name]",
            inputSchema={
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "SQL query using [database].[table] prefixes for all tables. Must follow Access SQL syntax rules.",
                    },
                },
                "required": ["sql"],
            },
        ),
        types.Tool(
            name="test_cross_db_connectivity",
            description="Test connectivity to all databases and verify cross-database query capability. Helps diagnose connection and syntax issues.",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": [],
            },
        ),
    ]

@server.call_tool()
async def handle_call_tool(name: str, arguments: dict[str, Any] | None) -> list[types.TextContent]:
    """Handle tool calls"""
    if arguments is None:
        arguments = {}
    
    if name == "list_databases":
        if not databases:
            result = "No databases registered"
        else:
            db_list = []
            db_list.append(f"Available Databases ({len(databases)}):")
            db_list.append("-" * 40)
            
            for db_name, db_info in databases.items():
                is_default = " (default)" if db_name == default_db_key else ""
                db_list.append(f"• {db_name}{is_default}")
                db_list.append(f"  Path: {db_info['path']}")
                db_list.append(f"  Description: {db_info['description']}")
                
                # Try to get basic info about the database
                try:
                    conn_str = (
                        r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
                        fr'DBQ={db_info["path"]};'
                    )
                    conn = pyodbc.connect(conn_str)
                    cursor = conn.cursor()
                    
                    # Count tables
                    table_count = len([row for row in cursor.tables(tableType='TABLE') if not row.table_name.startswith('MSys')])
                    db_list.append(f"  Tables: {table_count}")
                    conn.close()
                    
                except Exception as e:
                    db_list.append(f"  Status: Error - {str(e)}")
                
                db_list.append("")  # Empty line for spacing
            
            result = "\n".join(db_list)
    
    elif name == "get_schema_tool":
        database = arguments.get("database")
        format_type = arguments.get("format", "text")
        include_samples = arguments.get("include_samples", False)
        
        try:
            db_key, db_path = get_database_path(database)
        except ValueError as e:
            result = str(e)
        else:
            # Create a connection string
            conn_str = (
                r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
                fr'DBQ={db_path};'
            )
            
            # Establish the connection
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            
            # Get comprehensive schema information
            schema_info = {
                "database": db_key,
                "path": db_path,
                "description": databases[db_key]['description'],
                "tables": {},
                "relationships": [],
                "saved_queries": []
            }
            
            try:
                # Get all tables
                tables = []
                for row in cursor.tables(tableType='TABLE'):
                    if not row.table_name.startswith('MSys'):  # Skip system tables
                        tables.append(row.table_name)
                
                # Process each table
                for table_name in tables:
                    table_info = {
                        "columns": [],
                        "primary_keys": [],
                        "foreign_keys": [],
                        "indexes": [],
                        "sample_data": None,
                        "row_count": None
                    }
                    
                    # Get column information
                    columns = cursor.columns(table=table_name)
                    for column in columns:
                        col_info = {
                            "name": column.column_name,
                            "type": column.type_name,
                            "size": column.column_size,
                            "nullable": column.nullable,
                            "default": getattr(column, 'column_def', None),
                            "position": column.ordinal_position
                        }
                        table_info["columns"].append(col_info)
                    
                    # Get primary key information
                    try:
                        pk_columns = cursor.primaryKeys(table=table_name)
                        for pk in pk_columns:
                            table_info["primary_keys"].append({
                                "column": pk.column_name,
                                "key_seq": pk.key_seq
                            })
                    except:
                        pass
                    
                    # Get foreign key information
                    try:
                        fk_columns = cursor.foreignKeys(table=table_name)
                        for fk in fk_columns:
                            table_info["foreign_keys"].append({
                                "column": fk.fkcolumn_name,
                                "references_table": fk.pktable_name,
                                "references_column": fk.pkcolumn_name,
                                "constraint_name": getattr(fk, 'fk_name', 'Unknown')
                            })
                    except:
                        pass
                    
                    # Get table statistics
                    try:
                        count_result = cursor.execute(f"SELECT COUNT(*) FROM [{table_name}]").fetchone()
                        table_info["row_count"] = count_result[0] if count_result else 0
                    except:
                        table_info["row_count"] = "Unable to determine"
                    
                    # Get sample data if requested
                    if include_samples and table_info["row_count"] != 0:
                        try:
                            sample_result = cursor.execute(f"SELECT TOP 3 * FROM [{table_name}]").fetchall()
                            if sample_result:
                                # Convert to list of dictionaries for better readability
                                column_names = [col["name"] for col in table_info["columns"]]
                                samples = []
                                for row in sample_result:
                                    sample_row = {}
                                    for i, value in enumerate(row):
                                        if i < len(column_names):
                                            # Convert to string for JSON serialization
                                            sample_row[column_names[i]] = str(value) if value is not None else None
                                    samples.append(sample_row)
                                table_info["sample_data"] = samples
                        except:
                            table_info["sample_data"] = "Unable to retrieve sample data"
                    
                    schema_info["tables"][table_name] = table_info
                
                # Get saved queries/views (Access queries)
                try:
                    query_tables = cursor.tables(tableType='VIEW')
                    for query_row in query_tables:
                        if not query_row.table_name.startswith('MSys'):
                            query_info = {
                                "name": query_row.table_name,
                                "type": "VIEW/QUERY"
                            }
                            
                            # Try to get column information for the query
                            try:
                                query_columns = cursor.columns(table=query_row.table_name)
                                query_info["columns"] = []
                                for col in query_columns:
                                    query_info["columns"].append({
                                        "name": col.column_name,
                                        "type": col.type_name
                                    })
                            except:
                                query_info["columns"] = "Unable to retrieve query columns"
                            
                            schema_info["saved_queries"].append(query_info)
                except:
                    pass
                
                # Try to get relationship information from foreign keys
                relationships = {}
                for table_name, table_info in schema_info["tables"].items():
                    for fk in table_info["foreign_keys"]:
                        rel_key = f"{table_name}.{fk['column']} -> {fk['references_table']}.{fk['references_column']}"
                        relationships[rel_key] = {
                            "from_table": table_name,
                            "from_column": fk['column'],
                            "to_table": fk['references_table'],
                            "to_column": fk['references_column']
                        }
                schema_info["relationships"] = list(relationships.values())
                
            except Exception as e:
                schema_info["error"] = f"Error retrieving schema: {str(e)}"
            
            conn.close()
            
            if format_type.lower() == "json":
                result = json.dumps(schema_info, indent=2)
            else:
                # Return AI-friendly text format
                output = []
                output.append(f"DATABASE SCHEMA FOR AI AGENT")
                output.append(f"Database: {db_key} ({db_path})")
                output.append(f"Description: {databases[db_key]['description']}")
                output.append("=" * 80)
                
                # Tables section
                output.append(f"\nTABLES ({len(schema_info['tables'])} total):")
                output.append("-" * 40)
                
                for table_name, table_info in schema_info["tables"].items():
                    output.append(f"\n📋 TABLE: {table_name}")
                    output.append(f"   Rows: {table_info['row_count']}")
                    
                    # Primary keys
                    if table_info["primary_keys"]:
                        pk_cols = [pk["column"] for pk in table_info["primary_keys"]]
                        output.append(f"   🔑 Primary Key: {', '.join(pk_cols)}")
                    
                    # Columns
                    output.append("   📊 Columns:")
                    for col in table_info["columns"]:
                        nullable = "NULL" if col["nullable"] else "NOT NULL"
                        default_info = f", Default: {col['default']}" if col['default'] else ""
                        output.append(f"      • {col['name']}: {col['type']}({col['size']}) {nullable}{default_info}")
                    
                    # Foreign keys
                    if table_info["foreign_keys"]:
                        output.append("   🔗 Foreign Keys:")
                        for fk in table_info["foreign_keys"]:
                            output.append(f"      • {fk['column']} → {fk['references_table']}.{fk['references_column']}")
                    
                    # Sample data
                    if include_samples and table_info["sample_data"]:
                        output.append("   📋 Sample Data:")
                        if isinstance(table_info["sample_data"], list):
                            for i, sample in enumerate(table_info["sample_data"][:2]):  # Show max 2 samples
                                sample_str = ", ".join([f"{k}={v}" for k, v in sample.items() if v is not None])
                                output.append(f"      Row {i+1}: {sample_str}")
                        else:
                            output.append(f"      {table_info['sample_data']}")
                
                # Relationships section
                if schema_info["relationships"]:
                    output.append(f"\n🔗 RELATIONSHIPS ({len(schema_info['relationships'])} total):")
                    output.append("-" * 40)
                    for rel in schema_info["relationships"]:
                        output.append(f"   {rel['from_table']}.{rel['from_column']} → {rel['to_table']}.{rel['to_column']}")
                
                # Saved queries section
                if schema_info["saved_queries"]:
                    output.append(f"\n💾 SAVED QUERIES/VIEWS ({len(schema_info['saved_queries'])} total):")
                    output.append("-" * 40)
                    for query in schema_info["saved_queries"]:
                        output.append(f"   📝 {query['name']} ({query['type']})")
                        if isinstance(query['columns'], list):
                            col_names = [col['name'] for col in query['columns']]
                            output.append(f"      Columns: {', '.join(col_names)}")
                
                # AI Tips section
                output.append(f"\n🤖 AI QUERY TIPS:")
                output.append("-" * 40)
                output.append("• Use square brackets around table/column names: [TableName], [Column Name]")
                output.append("• Access uses different syntax: Use & for string concatenation, not +")
                output.append("• For dates, use # delimiters: WHERE [DateField] = #2023-01-01#")
                output.append("• Primary keys are ideal for WHERE clauses and JOINs")
                output.append("• Use foreign key relationships shown above for proper JOINs")
                if schema_info["saved_queries"]:
                    output.append("• Consider using saved queries/views as they may have complex logic")
                    output.append("• For queries across multiple databases, use [db_name].[TableName] syntax in your SQL, e.g. SELECT * FROM [db1].[Table1] JOIN [db2].[Table2] ON [db1].[Table1].[ID] = [db2].[Table2].[ID]. The tool will automatically handle the cross-database joining.")
                    output.append("• Use IIF(condition, true_value, false_value) instead of IF() for conditionals")
                    output.append("• No LIMIT clause; use TOP N in SELECT instead, e.g. SELECT TOP 10 * FROM [Table]")
                    output.append("• Use CInt(), CDbl() etc. for type conversions")
                    output.append("• For outer joins, use LEFT JOIN or RIGHT JOIN")
                
                result = "\n".join(output)
    
    elif name == "query_data":
        sql = arguments.get("sql")
        
        if not sql:
            result = "Error: SQL query is required"
        else:
            is_valid, validation_msg = validate_cross_db_syntax(sql, databases)
            if not is_valid:
                result = f"Error: {validation_msg}"
            else:
                db_key = default_db_key
                db_path = databases[db_key]['path']
                
                rewritten_sql = rewrite_cross_db_query(sql, databases, db_key)
                
                conn_str = (
                    r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
                    fr'DBQ={db_path};'
                    r'ExtendedAnsiSQL=1;'
                )
                
                try:
                    conn = pyodbc.connect(conn_str, timeout=30)
                    cursor = conn.cursor()
                    
                    print(f"DEBUG - Original SQL: {sql}")
                    print(f"DEBUG - Rewritten SQL: {rewritten_sql}")
                    print(f"DEBUG - Primary DB: {db_key}")
                    
                    query_result = cursor.execute(rewritten_sql).fetchall()
                    conn.close()
                    
                    output_lines = [f"Query executed on primary database: {db_key} with cross-DB support"]
                    output_lines.append("-" * 40)
                    output_lines.extend(str(row) for row in query_result)
                    
                    result = "\n".join(output_lines)
                    
                except pyodbc.Error as db_error:
                    error_code = getattr(db_error, 'args', ['Unknown'])[0]
                    error_msg = str(db_error)
                    
                    result = f"Database Error ({error_code}): {error_msg}\n"
                    result += f"Primary Database: {db_key}\n"
                    result += f"Original SQL: {sql}\n"
                    result += f"Rewritten SQL: {rewritten_sql}\n"
                    
                    if "Syntax error in FROM clause" in error_msg:
                        result += "\nTROUBLESHOOTING: Check database path and IN clause syntax"
                        result += f"\nVerify database paths exist:"
                        for db_name, db_info in databases.items():
                            exists = "✅" if os.path.exists(db_info['path']) else "❌"
                            result += f"\n  {exists} {db_name}: {db_info['path']}"
                    elif "Too few parameters" in error_msg:
                        result += "\nTROUBLESHOOTING: Check field names and boolean comparisons"
                        result += "\n  - Use 0/1 instead of True/False for boolean fields"
                        result += "\n  - Verify all field names exist in the tables"
                        result += "\n  - Check for typos in column names"
                    elif "No such table" in error_msg or "does not exist" in error_msg:
                        result += "\nTROUBLESHOOTING: Table name issue"
                        result += "\n  - Verify table names are spelled correctly"
                        result += "\n  - Check if table exists in the specified database"
                
                except Exception as e:
                    result = f"General Error: {str(e)}\nOriginal SQL: {sql}\nRewritten SQL: {rewritten_sql}"
    
    elif name == "test_cross_db_connectivity":
        test_results = []
        test_results.append("Cross-Database Connectivity Test")
        test_results.append("=" * 50)
        
        working_dbs = []
        for db_key, db_info in databases.items():
            try:
                if not os.path.exists(db_info['path']):
                    test_results.append(f"❌ {db_key}: File not found at {db_info['path']}")
                    continue
                    
                conn_str = (
                    r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
                    fr'DBQ={db_info["path"]};'
                )
                conn = pyodbc.connect(conn_str, timeout=10)
                
                cursor = conn.cursor()
                tables = [row.table_name for row in cursor.tables(tableType='TABLE') 
                         if not row.table_name.startswith('MSys')]
                
                test_results.append(f"✅ {db_key}: Connected successfully ({len(tables)} tables)")
                if tables:
                    test_results.append(f"   Sample tables: {', '.join(tables[:3])}{'...' if len(tables) > 3 else ''}")
                
                working_dbs.append((db_key, tables[0] if tables else None))
                conn.close()
                
            except Exception as e:
                test_results.append(f"❌ {db_key}: Connection failed - {str(e)}")
        
        if len(working_dbs) >= 2:
            test_results.append(f"\nTesting Cross-Database Query Rewriting:")
            test_results.append("-" * 30)
            
            db1_key, db1_table = working_dbs[0]
            db2_key, db2_table = working_dbs[1]
            
            if db1_table and db2_table:
                test_sql = f"SELECT COUNT(*) FROM [{db1_key}].[{db1_table}] UNION ALL SELECT COUNT(*) FROM [{db2_key}].[{db2_table}]"
                test_results.append(f"Original SQL: {test_sql}")
                
                rewritten = rewrite_cross_db_query(test_sql, databases, default_db_key)
                test_results.append(f"Rewritten SQL: {rewritten}")
                
                test_results.append("\nTesting rewritten query execution...")
                try:
                    conn_str = (
                        r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
                        fr'DBQ={databases[default_db_key]["path"]};'
                    )
                    conn = pyodbc.connect(conn_str, timeout=10)
                    cursor = conn.cursor()
                    result = cursor.execute(rewritten).fetchall()
                    test_results.append(f"✅ Cross-database query executed successfully!")
                    test_results.append(f"   Results: {len(result)} rows returned")
                    conn.close()
                except Exception as e:
                    test_results.append(f"❌ Cross-database query failed: {str(e)}")
        else:
            test_results.append(f"\nNeed at least 2 working databases to test cross-DB queries")
            test_results.append(f"Currently have {len(working_dbs)} working database(s)")
        
        result = "\n".join(test_results)
    
    else:
        result = f"Unknown tool: {name}"
    
    return [types.TextContent(type="text", text=result)]

def create_sse_server():
    """Create a Starlette app that handles SSE connections"""
    transport = SseServerTransport("/messages")
    
    async def handle_sse(request):
        async with transport.connect_sse(
            request.scope, request.receive, request._send
        ) as streams:
            await server.run(
                streams[0], streams[1], InitializationOptions(
                    server_name="MS Access Explorer",
                    server_version="0.1.0",
                    capabilities=server.get_capabilities(
                        notification_options=NotificationOptions(),
                        experimental_capabilities={},
                    ),
                )
            )
    
    routes = [
        Route("/sse", endpoint=handle_sse),
        Mount("/messages", app=transport.handle_post_message),
    ]
    
    return Starlette(routes=routes)

async def run_stdio():
    """Run the server with stdio transport"""
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="MS Access Explorer",
                server_version="0.1.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )

def run():
    """Run the MCP server with configured transport"""
    if args.transport == 'stdio':
        print("Starting MS Access MCP server with stdio transport...")
        asyncio.run(run_stdio())
    elif args.transport == 'sse':
        print(f"Starting MS Access MCP server with SSE transport on {args.host}:{args.port}{args.path}")
        app = create_sse_server()
        uvicorn.run(app, host=args.host, port=args.port)
    else:
        print(f"Unknown transport: {args.transport}")
        sys.exit(1)

if __name__ == "__main__":
    run() 