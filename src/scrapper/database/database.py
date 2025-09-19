from .sql_db import SqlDatabaseService
from .orm_db import OrmDatabaseService
from .db_service import DatabaseService
import os
from dotenv import load_dotenv

load_dotenv()

def create_database_service() -> DatabaseService:
    access_type = os.getenv("ACCESS_TYPE", "ORM").upper()
    # access_type = "SQL"

    if access_type == "SQL":
        return SqlDatabaseService()
    elif access_type == "ORM":
        return OrmDatabaseService()
    else:
        raise ValueError(f"Invalid access-type: {access_type}. Must be 'SQL' or 'ORM'.")