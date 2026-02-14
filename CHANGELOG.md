## Unreleased

### Feat

- **05-07**: add progress indicators and verbose mode
- **05-07**: add structured JSON errors and --json flag infrastructure
- **05-04**: add --skip-quality flag, quality summary, and build-quality tests
- **05-06**: add freshness CLI command and tests
- **05-06**: create freshness calculation module
- **05-04**: add validate-before-write and result persistence to BuildEngine
- **05-05**: add strata quality CLI command
- **05-02**: implement validation engine with pluggable constraint checker
- **05-03**: implement SQLite quality and build record persistence
- **05-03**: add quality and build data types to registry
- **05-01**: rework SLA model and add Field severity
- **04-07**: update compile command to use enhanced output
- **04-07**: create compile output module
- **04-07**: create schema evolution detection module
- **04-06**: add build command to CLI
- **04-05**: create BuildEngine pydantic model for feature materialization
- **04-03**: replace StorageKind + ComputeKind with BackendKind
- **04-03**: create DuckDBBackend with Ibis connection and format delegation
- **04-03**: add BaseBackend to replace BaseStorage + BaseCompute
- **04-03**: implement format classes with read/write logic
- **04-04**: create DAG class with dependency graph building
- **04-01**: implement custom feature and transform compilation
- **04-01**: implement aggregate compilation with schema inference
- **04-01**: create IbisCompiler class with CompiledQuery dataclass
- **04-02**: add write semantics fields to FeatureTable
- export LocalSourceConfig and plugin type aliases from public API
- implement smart auto-discovery for feature definitions
- **03-07**: implement strata down and ls commands
- **03-05**: implement validate and compile CLI commands
- **03-04**: implement preview and up CLI commands
- **03-03**: create diff module with Change representation
- **03-02**: create discovery module with DefinitionDiscoverer class
- **03-01**: implement SqliteRegistry with hybrid schema
- **03-01**: update BaseRegistry interface
- **03-01**: create registry data types module
- **02-07**: enhance Dataset class with feature naming support
- **02-06**: implement feature() and transform() decorators
- **02-06**: implement aggregate() method
- **02-05**: enhance FeatureTable with DAG support
- **02-04**: enhance SourceTable with feature access
- **02-01**: enhance Schema to work with Field class
- **02-03**: enhance source types with proper config typing
- **02-02**: create LocalSourceConfig and plugin config tests
- **02-01**: enhance Entity with join_keys validation
- **02-02**: create DuckDBSourceConfig
- **02-02**: add BaseSourceConfig to plugin base classes
- Phase 1 Foundation - configuration system and CLI

### Fix

- **05-04**: fix build tests with mock backend data
- **03-07**: update air-quality example to use LocalSourceConfig
- **03-07**: reject SourceTable in compile command
- **02-04**: resolve circular import between core and checks

### Refactor

- simplify Phase 5 quality/build/CLI code
- **05-02**: clean up validation engine
- simplify Phase 4 compiler/build code
- **04-03**: rename plugins/ to backends/ with import updates
- simplify Phase 3 code
