import pydantic as pdt


class BaseSource(pdt.BaseModel):
    pass


class SourceConfig(pdt.BaseModel):
    pass


class LocalConfig(SourceConfig):
    path: str
    format: str


class BatchSource(BaseSource):
    name: str
    description: str
    config: SourceConfig
    timestamp_field: str


class StreamSource(BaseSource):
    name: str
    description: str
    config: SourceConfig
    timestamp_field: str
    batch_output: SourceConfig


class RealTimeSource(BaseSource):
    name: str
    description: str
    config: SourceConfig
    timestamp_field: str


SourceKind = BatchSource | StreamSource | RealTimeSource
