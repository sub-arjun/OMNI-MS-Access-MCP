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
parser.add_argument('--transport', default='stdio', choices=['stdio', 'sse', 'http'], help='Transport method (default: stdio)')
parser.add_argument('--host', default='127.0.0.1', help='Host for HTTP/SSE (default: 127.0.0.1)')
parser.add_argument('--port', type=int, default=8000, help='Port for HTTP/SSE (default: 8000)')
parser.add_argument('--path', default=None, help='Path for SSE/HTTP endpoint (default: /sse for SSE, /mcp for HTTP)')
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
def get_schema_tool(database: str = None, format: str = "text", include_samples: bool = False) -> str:
    """Get comprehensive database schema information for AI agents
    
    Args:
        database: Name of the database to query (uses default if not specified)
        format: Output format - 'text' or 'json'
        include_samples: Whether to include sample data for each table (helps AI understand data patterns)
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
        
        # Try to get relationship information from system tables
        try:
            # This may not work in all Access versions due to permissions
            rel_query = """
            SELECT 
                r.szRelationship as relationship_name,
                r.szTable as from_table,
                r.szColumn as from_column,
                r.szReferencedTable as to_table,
                r.szReferencedColumn as to_column
            FROM MSysRelationships r
            """
            rel_result = cursor.execute(rel_query).fetchall()
            for rel in rel_result:
                schema_info["relationships"].append({
                    "name": rel[0],
                    "from_table": rel[1],
                    "from_column": rel[2],
                    "to_table": rel[3],
                    "to_column": rel[4]
                })
        except:
            # If we can't access system tables, build relationships from foreign keys
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
    
    if format.lower() == "json":
        return json.dumps(schema_info, indent=2)
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
                rel_name = rel.get('name', 'Unnamed')
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
        
        return "\n".join(output)


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
    """Run the MCP server with configured transport"""
    if args.transport == 'stdio':
        mcp.run()
    else:
        run_kwargs = {
            'transport': args.transport,
            'host': args.host,
            'port': args.port
        }
        if args.path:
            run_kwargs['path'] = args.path
        elif args.transport == 'sse':
            run_kwargs['path'] = '/sse'
        elif args.transport == 'http':
            run_kwargs['path'] = '/mcp'
        
        mcp.run(**run_kwargs)


if __name__ == "__main__":
    run() 