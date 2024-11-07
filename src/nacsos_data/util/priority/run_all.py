from typing import TYPE_CHECKING

from nacsos_data.db import get_engine_async
from nacsos_data.models.nql import AssignmentFilter
from nacsos_data.util.annotations.export import wide_export_table
from pydantic_settings import BaseSettings

from nacsos_data.util.priority.mask import get_inclusion_mask

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession  # noqa: F401


class PrioritySettings(BaseSettings):
    # ID of priority config
    PRIORITY_ID: str

    # Directory (assuming absolute path) where to put results
    OUT_DIR: str


async def main() -> None:
    db_engine = get_engine_async(conf_file='../../nacsos-core/config/local.env')

    async with db_engine.session() as session:  # type: AsyncSession
        base_cols, label_cols, t = await wide_export_table(session=session,
                                                           scope_ids=[
                                                               "8f8fc0db-8568-4e95-ae96-81831e3ef33d",
                                                               "5c09e359-02a3-47da-afcc-70ea5e9ce4b2",
                                                               "759fbf3f-d703-4f26-8779-c93cfd0dc7de",
                                                               "61553975-07bb-4b53-ae87-4236945b2a54",
                                                               "82b62c14-0500-4a2c-a4a0-022bcc8c5f2c",
                                                               "4318ce37-2223-4205-b67d-20cde31e4d04",
                                                               "6fd80e6c-a425-4f91-8cbc-47c12852316e",
                                                               "b9cacd53-6e80-4a04-b6aa-083da63cb752"
                                                           ],
                                                           limit=None,
                                                           project_id='3e87c64e-115b-42cb-8992-b266700eebd1',
                                                           nql_filter=AssignmentFilter(filter='assignment', scopes=None, mode=1,
                                                                                       scheme=None))
        incl = get_inclusion_mask(df=t, rule='res|rel:1', label_cols=label_cols)
        print('is na', t['res|rel:1'].isna().sum())
        t['incl'] = incl
        # seen = ~incl.isna()
        print(incl)
        print(t.shape)


if __name__ == '__main__':
    import asyncio

    asyncio.run(main())
