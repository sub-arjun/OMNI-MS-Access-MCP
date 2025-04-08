#!/usr/bin/env python3
import argparse
import sys
from omni_ms_access_mcp.server import AccessMCP

def main():
    """Command line entry point for the MS Access MCP tool"""
    parser = argparse.ArgumentParser(description="MS Access Explorer MCP Server")
    parser.add_argument("--db-path", type=str, required=True, 
                        help="Path to the Access database file (.accdb or .mdb)")
    args = parser.parse_args()
    
    try:
        # Initialize the MCP server
        mcp_server = AccessMCP(db_path=args.db_path)
        
        # Run the server
        print(f"Starting MS Access MCP server with database: {args.db_path}")
        mcp_server.run()
    except FileNotFoundError as e:
        print(f"Error: {str(e)}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 