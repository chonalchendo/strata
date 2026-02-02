# clean build folders
[group('clean')]
clean-build:
    rm -rf dist/
    rm -rf build/

# clean constraints file
[group('clean')]
clean-constraints:
    rm -rf constraints.txt
