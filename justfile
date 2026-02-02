docker := require("docker")
rm := require("rm")
uv := require("uv")


PACKAGE := "strata"
REPOSITORY := "strata"
SOURCES := "src"
TESTS := "tests"

default:
    @just --list

import "tasks/check.just"
import "tasks/clean.just"
import "tasks/commit.just"
import "tasks/docs.just"
import "tasks/format.just"
import "tasks/install.just"
import "tasks/package.just"
