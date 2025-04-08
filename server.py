import pyodbc
import argparse
import os
import sys
import json

from mcp.server.fastmcp import FastMCP

# Parse command-line arguments
parser = argparse.ArgumentParser(description="MS Access Explorer MCP Server")
parser.add_argument("--db-path", type=str, help="Path to the Access database file (.accdb or .mdb)")
args = parser.parse_args()

# Get the database path
db_path = args.db_path
if not db_path:
    print("Error: Database path not provided. Use --db-path argument.")
    print("Example: mcp-cli run server.py --db-path \"C:\\path\\to\\database.accdb\"")
    sys.exit(1)

# Verify the database exists
if not os.path.exists(db_path):
    print(f"Error: Database file not found at {db_path}")
    sys.exit(1)

mcp = FastMCP("MS Access Explorer")


@mcp.resource("schema://main")
def get_schema() -> str:
    """Provide the database schema as a resource"""
    # Create a connection string
    conn_str = (
        r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
        fr'DBQ={db_path};'
    )
    
    # Establish the connection
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    
    # Get table schema information
    tables = []
    for row in cursor.tables(tableType='TABLE'):
        table_name = row.table_name
        tables.append(f"Table: {table_name}")
        # Get column information for each table
        columns = cursor.columns(table=table_name)
        for column in columns:
            tables.append(f"  - Column: {column.column_name}, Type: {column.type_name}")
    
    return "\n".join(tables)


@mcp.tool()
def get_schema_tool(format: str = "text") -> str:
    """Get the database schema
    
    Args:
        format: Output format - 'text' or 'json'
    """
    # Create a connection string
    conn_str = (
        r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
        fr'DBQ={db_path};'
    )
    
    # Establish the connection
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    
    if format.lower() == "json":
        # Return schema as JSON
        schema_data = {}
        for row in cursor.tables(tableType='TABLE'):
            table_name = row.table_name
            schema_data[table_name] = []
            
            # Get column information for each table
            columns = cursor.columns(table=table_name)
            for column in columns:
                schema_data[table_name].append({
                    "name": column.column_name,
                    "type": column.type_name,
                    "nullable": column.nullable,
                    "size": column.column_size
                })
        
        return json.dumps(schema_data, indent=2)
    else:
        # Return schema as text (default)
        tables = []
        for row in cursor.tables(tableType='TABLE'):
            table_name = row.table_name
            tables.append(f"Table: {table_name}")
            
            # Get column information for each table
            columns = cursor.columns(table=table_name)
            for column in columns:
                nullable = "NULL" if column.nullable else "NOT NULL"
                tables.append(f"  - Column: {column.column_name}, Type: {column.type_name}({column.column_size}), {nullable}")
        
        return "\n".join(tables)


@mcp.tool()
def query_data(sql: str) -> str:
    """Execute SQL queries safely"""
    # Create a connection string
    conn_str = (
        r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
        fr'DBQ={db_path};'
    )
    
    # Establish the connection
    conn = pyodbc.connect(conn_str)
    try:
        cursor = conn.cursor()
        result = cursor.execute(sql).fetchall()
        conn.close()
        return "\n".join(str(row) for row in result)
    except Exception as e:
        return f"Error: {str(e)}"
