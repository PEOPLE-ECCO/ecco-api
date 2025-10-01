import datetime
import enum
import uuid
from typing import Optional

import sqlalchemy
from sqlalchemy import Column, String, func, Index, Integer, Sequence, ForeignKey
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import Mapped, DeclarativeBase, mapped_column


class Base(DeclarativeBase):
    ...


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = Column(Integer, Sequence('seq_user_id', start=10, increment=1), primary_key=True)
    name: Mapped[str] = mapped_column(String(30), nullable=False)
    mail: Mapped[str] = mapped_column(nullable=False)
    kc_uuid: Mapped[uuid] = Column(sqlalchemy.types.UUID, index=True, nullable=False)

    def as_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "mail": self.mail
        }


class Scenario(Base):
    __tablename__ = "scenarios"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(30))
    description: Mapped[Optional[str]]
    preview_image: Mapped[Optional[str]]
    acl_read: Mapped[[uuid]] = Column(ARRAY(sqlalchemy.types.UUID), nullable=False)

    # TODO link processes and scenarios

    __table_args__ = (
        Index(f"{__tablename__}_acl_read_index", "acl_read", postgresql_using="gin"),
    )

    def as_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "preview_image": self.preview_image,
            "description": self.description
        }


class JobStatus(enum.Enum):
    INIT = enum.auto(),
    CREATED = enum.auto(),
    QUEUED = enum.auto(),
    RUNNING = enum.auto(),
    AWAITING_RESULT_DOWNLOAD = enum.auto(),
    COMPLETED = enum.auto(),
    CANCELED = enum.auto(),
    ERROR = enum.auto()


class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[int] = mapped_column(primary_key=True)
    flow_run_id: Mapped[uuid.UUID]
    timeseries_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("timeseries.id"))
    scheduleTime: Mapped[datetime.datetime] = mapped_column(server_default=func.now())

    credits: Mapped[Optional[float]]
    user_id = Column(Integer, ForeignKey(User.id))
    acl_read: Mapped[[uuid]] = Column(ARRAY(sqlalchemy.types.UUID), nullable=False)

    __table_args__ = (
        Index(f"{__tablename__}_acl_read_index", "acl_read", postgresql_using="gin"),
    )

    def as_dict(self):
        return {
            "id": self.id,
            "timeseries_id": self.timeseries_id,
            "credits": self.credits,
        }


class Timeseries(Base):
    __tablename__ = "timeseries"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(30))
    scenario_id = Column(Integer, ForeignKey(Scenario.id))
    description: Mapped[str]
    bucket: Mapped[str]

    process = Column(Integer, ForeignKey("processes.id"))
    process_parameters: Mapped[Optional[dict]] = Column(JSONB)

    # jobs: Mapped[List["Job"]] = relationship(back_populates="timeseries")

    acl_read: Mapped[[uuid]] = Column(ARRAY(sqlalchemy.types.UUID), nullable=False)
    __table_args__ = (
        Index(f"{__tablename__}_acl_read_index", "acl_read", postgresql_using="gin"),
    )

    def as_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "scenario_id": self.scenario_id,
            "description": self.description,
            "process": self.process,
            "process_parameters": self.process_parameters,
        }


class Process(Base):
    __tablename__ = "processes"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(30))
    scenario_id = Column(Integer, ForeignKey(Scenario.id), nullable=False)
    description: Mapped[Optional[str]]
    parameters = Column(JSONB)

    acl_read: Mapped[[uuid]] = Column(ARRAY(sqlalchemy.types.UUID), nullable=False)

    __table_args__ = (
        Index(f"{__tablename__}_acl_read_index", "acl_read", postgresql_using="gin"),
    )

    def as_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            #            "scenario_id": self.scenario_id,
            "git_commit": self.git_commit,
            "git_repo": self.git_repo,
            "git_location": self.git_location,
            "description": self.description,
            "parameters": self.parameters,
        }
