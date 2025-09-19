from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, TIMESTAMP, Identity, CheckConstraint
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import MetaData
from typing import List
from .db_service import DatabaseService
import logging
from dotenv import load_dotenv
import os

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

LOG_FILE = os.path.join("logs", "notification.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename=LOG_FILE,
)
logger = logging.getLogger(__name__)

# DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
DATABASE_URL = f"postgresql://user:password@db:5432/mydb"

engine = create_engine(DATABASE_URL)

Base = declarative_base()

class User(Base):
    __tablename__ = "users"

    user_id = Column(Integer, Identity(), primary_key=True)
    telegram_id = Column(Integer, unique=True, nullable=False)

    subscriptions = relationship("Subscription", back_populates="user")

class Link(Base):
    __tablename__ = "links"

    link_id = Column(Integer, Identity(), primary_key=True)
    url = Column(String(2048), unique=True, nullable=False)
    type = Column(String(50), nullable=False)
    last_checked_at = Column(TIMESTAMP(timezone=False), nullable=False)

    __table_args__ = (
        CheckConstraint(type.in_(['stackoverflow', 'github']), name='links_type_check'),
    )

    subscriptions = relationship("Subscription", back_populates="link")
    updates = relationship("Update", back_populates="link")


class Subscription(Base):
    __tablename__ = "subscriptions"

    subscription_id = Column(Integer, Identity(), primary_key=True)
    user_id = Column(Integer, ForeignKey("users.user_id"), nullable=False)
    link_id = Column(Integer, ForeignKey("links.link_id"), nullable=False)
    created_at = Column(TIMESTAMP(timezone=False), nullable=False)

    user = relationship("User", back_populates="subscriptions")
    link = relationship("Link", back_populates="subscriptions")


class Update(Base):
    __tablename__ = "updates"

    update_id = Column(Integer, Identity(), primary_key=True)
    link_id = Column(Integer, ForeignKey("links.link_id"), nullable=False)
    created_at = Column(TIMESTAMP(timezone=False), nullable=False)
    title = Column(String(255))
    user_name = Column(String(255))
    preview = Column(String)

    link = relationship("Link", back_populates="updates")

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

class OrmDatabaseService(DatabaseService):
    def __init__(self):
        self.engine = create_engine(DATABASE_URL)
        Base.metadata.create_all(bind=self.engine)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)

    def get_users_by_link_id(self, link_id: int, offset: int, limit: int) -> List[int]:
        db = self.SessionLocal()
        try:
            users = db.query(User.telegram_id).join(Subscription).filter(Subscription.link_id == link_id).offset(offset).limit(limit).all()
            telegram_ids = [user[0] for user in users]
            return telegram_ids
        except Exception as e:
            logger.exception(f"Failed to get users list for link {link_id}: {e}")
            raise
        finally:
            db.close()