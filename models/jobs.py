from pydantic import BaseModel


class JobResponse(BaseModel):
    id: str
    file_path: str
    collection_name: str
