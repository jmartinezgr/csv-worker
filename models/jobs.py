from pydantic import BaseModel, Field


class JobConfig(BaseModel):
    """Configuration for CSV processing job.

    Attributes:
        delimitador: CSV delimiter/separator (default: ",")
        size: Batch size for processing chunks (number of rows)
        campos: List of column names to use (if None, reads from header)
        reglas: Column data type rules (dict mapping column name to pandas dtype string)
        codec: File encoding (default: "utf-8")
        skiprows: Number of rows to skip at the beginning (default: 0)
        merror: Encoding error handling strategy ("strict", "ignore", "replace")
        codigo: Optional code/identifier for the processing type
        type_head: Header type - "auto", "file_parse", "file_fixed" (legacy compatibility)
    """

    delimitador: str | None = Field(None, description="CSV delimiter/separator")
    size: str | None = Field(None, description="Batch/chunk size for processing")
    campos: list[str] | None = Field(None, description="Column names to apply")
    reglas: dict[str, str] | None = Field(
        None, description="Column type rules (pandas dtypes)"
    )
    codec: str | None = Field(None, description="File encoding (e.g., utf-8, latin-1)")
    skiprows: int | None = Field(None, description="Number of rows to skip")
    merror: str | None = Field(
        None, description="Encoding error handling (strict/ignore/replace)"
    )
    codigo: str | None = Field(None, description="Processing type code")
    type_head: str | None = Field(
        None, description="Header type: auto/file_parse/file_fixed"
    )


class JobResponse(BaseModel):
    id: str
    file_path: str
    collection_name: str
    client_id: int
    period: str
    config: JobConfig
