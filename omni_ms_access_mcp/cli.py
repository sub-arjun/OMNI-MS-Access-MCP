#!/usr/bin/env python3
import sys
import os
from omni_ms_access_mcp.server import run

def main():
    """Command line entry point for the MS Access MCP tool"""
    # The server module already handles argument parsing
    run()

if __name__ == "__main__":
    main() 