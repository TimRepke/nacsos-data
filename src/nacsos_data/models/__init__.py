from nacsos_data.models.web_of_science import BaseModel


class ScopeInfo(BaseModel):
    scope_id: str
    scope_name: str
    scheme_id: str
    scheme_name: str
