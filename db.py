# db.py
import sqlite3
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text, Boolean, BigInteger, ForeignKey
from sqlalchemy.orm import declarative_base, sessionmaker
from config import DB_NAME

Base = declarative_base()

class Message(Base):
    __tablename__ = 'messages'

    id = Column(Integer, primary_key=True, autoincrement=True)
    message_id = Column(BigInteger, unique=True)
    user_id = Column(BigInteger, index=True)
    username = Column(String)
    date = Column(DateTime, index=True)
    text = Column(Text)
    media_type = Column(String)
    duration = Column(Integer, default=0)
    reaction_count = Column(Integer, default=0)
    
    # ВОТ ЭТА СТРОЧКА, КОТОРОЙ НЕ ХВАТАЛО:
    reply_to_msg_id = Column(BigInteger, nullable=True)
    
    # Флаги
    is_forwarded = Column(Boolean, default=False)
    
    # NLP
    text_len = Column(Integer, default=0)
    word_count = Column(Integer, default=0)
    has_question = Column(Boolean, default=False)
    has_caps = Column(Boolean, default=False)
    has_laugh = Column(Boolean, default=False)
    sentiment_score = Column(Integer, default=0)

# Таблица упоминаний (без изменений)
class Mention(Base):
    __tablename__ = 'mentions'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    source_username = Column(String)
    target_name = Column(String)
    date = Column(DateTime)

engine = create_engine(f'sqlite:///{DB_NAME}', echo=False)

def init_db():
    Base.metadata.create_all(engine)

SessionLocal = sessionmaker(bind=engine)