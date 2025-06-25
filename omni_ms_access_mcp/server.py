import pyodbc
import os
import json
import argparse
import sys

from mcp.server.fastmcp import FastMCP

# Parse command-line arguments
parser = argparse.ArgumentParser(description="MS Access Explorer MCP Server")
parser.add_argument("--db-path", type=str, action='append', dest='db_paths', 
                   help="Path to Access database file (.accdb or .mdb). Use multiple times for multiple databases.")
parser.add_argument("--db-name", type=str, action='append', dest='db_names',
                   help="Friendly name for the database. Must match the order of --db-path arguments.")
parser.add_argument("--db-desc", type=str, action='append', dest='db_descriptions',
                   help="Description for the database. Must match the order of --db-path arguments.")
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
mcp = FastMCP("MS Access Explorer")


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


@mcp.resource("schema://main")
def get_schema() -> str:
    """Provide the database schema as a resource"""
    # Create a connection string
    conn_str = (
        r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
        fr'DBQ={databases[default_db_key]["path"]};'
    )
    
    # Establish the connection
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    
    # Get table schema information
    tables = [f"Database: {default_db_key}"]
    for row in cursor.tables(tableType='TABLE'):
        table_name = row.table_name
        tables.append(f"Table: {table_name}")
        # Get column information for each table
        columns = cursor.columns(table=table_name)
        for column in columns:
            tables.append(f"  - Column: {column.column_name}, Type: {column.type_name}")
    
    return "\n".join(tables)


@mcp.tool()
def list_databases() -> str:
    """List all available databases
    
    Returns:
        str: List of database names and their file paths
    """
    if not databases:
        return "No databases registered"
    
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
            table_count = len([row for row in cursor.tables(tableType='TABLE')])
            db_list.append(f"  Tables: {table_count}")
            conn.close()
            
        except Exception as e:
            db_list.append(f"  Status: Error - {str(e)}")
        
        db_list.append("")  # Empty line for spacing
    
    return "\n".join(db_list)


@mcp.tool()
def get_schema_tool(database: str = None, format: str = "text") -> str:
    """Get the database schema
    
    Args:
        database: Name of the database to query (uses default if not specified)
        format: Output format - 'text' or 'json'
    """
    try:
        db_key, db_path = get_database_path(database)
    except ValueError as e:
        return str(e)
    
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
        schema_data = {
            "database": db_key,
            "path": db_path,
            "description": databases[db_key]['description'],
            "tables": {}
        }
        for row in cursor.tables(tableType='TABLE'):
            table_name = row.table_name
            schema_data["tables"][table_name] = []
            
            # Get column information for each table
            columns = cursor.columns(table=table_name)
            for column in columns:
                schema_data["tables"][table_name].append({
                    "name": column.column_name,
                    "type": column.type_name,
                    "nullable": column.nullable,
                    "size": column.column_size
                })
        
        return json.dumps(schema_data, indent=2)
    else:
        # Return schema as text (default)
        tables = [f"Database: {db_key} ({db_path})"]
        tables.append(f"Description: {databases[db_key]['description']}")
        tables.append("=" * 50)
        
        for row in cursor.tables(tableType='TABLE'):
            table_name = row.table_name
            tables.append(f"\nTable: {table_name}")
            
            # Get column information for each table
            columns = cursor.columns(table=table_name)
            for column in columns:
                nullable = "NULL" if column.nullable else "NOT NULL"
                tables.append(f"  - Column: {column.column_name}, Type: {column.type_name}({column.column_size}), {nullable}")
        
        return "\n".join(tables)


@mcp.tool()
def query_data(sql: str, database: str = None) -> str:
    """Execute SQL queries safely
    
    Args:
        sql: SQL query to execute
        database: Name of the database to query (uses default if not specified)
    """
    try:
        db_key, db_path = get_database_path(database)
    except ValueError as e:
        return str(e)
    
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
        
        # Add database info to output
        output_lines = [f"Query executed on database: {db_key}"]
        output_lines.append("-" * 40)
        output_lines.extend(str(row) for row in result)
        
        return "\n".join(output_lines)
    except Exception as e:
        return f"Error querying database '{db_key}': {str(e)}"


def run():
    """Run the MCP server"""
    mcp.run()


if __name__ == "__main__":
    run() 