@echo off
echo Building and publishing OMNI-MS-Access-MCP

echo Creating a clean build environment...
rmdir /s /q dist build 2>nul
rmdir /s /q omni_ms_access_mcp.egg-info 2>nul

echo Building the package...
uv pip install -U build
uv python -m build

echo Package built successfully!
echo.
echo To publish to PyPI, run:
echo uv pip install -U twine
echo uv python -m twine upload dist/*
echo.
echo To publish to a private repository (like UVX), run:
echo uv python -m twine upload --repository-url YOUR_REPOSITORY_URL dist/* 