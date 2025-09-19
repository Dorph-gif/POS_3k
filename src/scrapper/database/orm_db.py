from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, TIMESTAMP, Identity, CheckConstraint
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import MetaData
from typing import List, Dict
from .db_service import DatabaseService
from sqlalchemy import select
import datetime
import logging
from dotenv import load_dotenv
import os

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

LOG_FILE = os.path.join("logs", "scrapper.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename=LOG_FILE,
)
logger = logging.getLogger(__name__)

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

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

    async def add_subscription(self, telegram_id: int, url: str, tags: list = None, filters: list = None) -> None:
        db = self.SessionLocal()
        try:
            user = db.query(User).filter(User.telegram_id == telegram_id).first()

            if not user:
                user = User(telegram_id=telegram_id)
                db.add(user)
                db.commit()

            user_id = user.user_id

            link = db.query(Link).filter(Link.url == url).first()

            if link:
                link_id = link.link_id
            else:
                if "stackoverflow.com" in url:
                    link_type = "stackoverflow"
                elif "github.com" in url:
                    link_type = "github"
                else:
                    raise ValueError("Invalid URL type")

                link = Link(url=url, type=link_type, last_checked_at=datetime.datetime.utcnow())
                db.add(link)
                db.commit()
                link_id = link.link_id

            subscription = Subscription(user_id=user_id, link_id=link_id, created_at=datetime.datetime.utcnow())
            db.add(subscription)
            db.commit()

        except Exception as e:
            db.rollback()
            print(f"Error adding subscription: {e}")
            raise
        finally:
            db.close()

    async def delete_subscription(self, telegram_id: int, url: str) -> None:
        db = self.SessionLocal()
        try:
            user = db.query(User).filter(User.telegram_id == telegram_id).first()

            if not user:
                user = User(telegram_id=telegram_id)
                db.add(user)
                db.commit()

            user_id = user.user_id

            link = db.query(Link).filter(Link.url == url).first()

            if link:
                subscription = db.query(Subscription).filter(Subscription.user_id == user_id, Subscription.link_id == link.link_id).first()
                if subscription:
                    db.delete(subscription)
                    db.commit()
            else:
                print(f"Link with URL '{url}' not found.")
                raise ValueError(f"Link with URL '{url}' not found.")

        except Exception as e:
            db.rollback()
            print(f"Error deleting subscription: {e}")
            raise
        finally:
            db.close()

    async def get_subscriptions(self, telegram_id: int) -> List[Dict]:
        db = self.SessionLocal()
        try:
            user = db.query(User).filter(User.telegram_id == telegram_id).first()

            if not user:
                user = User(telegram_id=telegram_id)
                db.add(user)
                db.commit()

            user_id = user.user_id
            
            subscriptions = db.query(Subscription).join(Link).filter(Subscription.user_id == user_id).all()

            subscription_list = []
            for subscription in subscriptions:
                subscription_list.append({
                    "url": subscription.link.url,
                    "type": subscription.link.type,
                    "created_at": subscription.created_at.isoformat()
                })

            return subscription_list
        except Exception as e:
            print(f"Error getting subscriptions: {e}")
            raise
        finally:
            db.close()

    async def get_links(self, offset: int, limit: int) -> List[Dict]:
        db = self.SessionLocal()

        try:
            links = db.query(Link).order_by(Link.last_checked_at).offset(offset).limit(limit).all()
            return [link.__dict__ for link in links]
        except Exception as e:
            print(f"Error getting links: {e}")
            raise
        finally:
            db.close()

    async def update_last_checked_at(self, link_id: int) -> None:
        db = self.SessionLocal()

        try:
            link = db.query(Link).filter(Link.link_id == link_id).first()
            if link:
                link.last_checked_at = datetime.datetime.utcnow()
                
                db.commit()
            else:
                print(f"Link with id {link_id} not found.")
        except Exception as e:
            db.rollback()
            print(f"Error updating last_checked_at for link_id {link_id}: {e}")
            raise
        finally:
            db.close()

    async def get_all_user_ids(self) -> List[int]:
        db = self.SessionLocal()
        try:
            user_ids = [user_id[0] for user_id in db.query(User.user_id).distinct().all()]
            return user_ids
        except Exception as e:
            print(f"Error getting all user IDs: {e}")
            raise
        finally:
            db.close()