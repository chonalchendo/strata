# run format tasks
[group('format')]
format: format-import format-source

# format python code import
[group('format')]
format-import:
    uv run ruff check --select=I --fix {{SOURCES}} {{TESTS}}

# format python code source
[group('format')]
format-source:
    uv run ruff format {{SOURCES}} {{TESTS}}
