import datetime
import enum
import uuid
from typing import Optional, List

from sqlalchemy import Column, String, func, Index, Integer, Sequence, ForeignKey
from sqlalchemy.dialects.postgresql import ARRAY, UUID, JSONB
from sqlalchemy.orm import Mapped, DeclarativeBase, mapped_column, relationship


class Base(DeclarativeBase):
    ...


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = Column(Integer, Sequence('seq_user_id', start=10, increment=1), primary_key=True)
    name: Mapped[str] = mapped_column(String(30), nullable=False)
    mail: Mapped[str] = mapped_column(nullable=False)
    kc_uuid: Mapped[uuid] = Column(UUID, index=True, nullable=False)

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
    acl_read: Mapped[[uuid]] = Column(ARRAY(UUID), nullable=False)

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
    SCHEDULED = enum.auto(),
    RUNNING = enum.auto(),
    COMPLETED = enum.auto(),
    CANCELED = enum.auto(),
    ERROR = enum.auto()


class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[int] = mapped_column(primary_key=True)
    status: Mapped[JobStatus]
    timeseries_id: Mapped[UUID] = mapped_column(ForeignKey("timeseries.id"))
    timeseries: Mapped["Timeseries"] = relationship(back_populates="jobs")
    scheduleTime: Mapped[datetime.datetime] = mapped_column(server_default=func.now())
    executionTimeStart: Mapped[Optional[datetime.datetime]]
    executionTimeEnd: Mapped[Optional[datetime.datetime]]
    credits: Mapped[Optional[float]]
    log: Mapped[Optional[str]]
    catalog: Mapped[Optional[str]]
    #     properties: Mapped[Optional[dict]] = Column(JSONB)
    acl_read: Mapped[[uuid]] = Column(ARRAY(UUID), nullable=False)

    __table_args__ = (
        Index(f"{__tablename__}_acl_read_index", "acl_read", postgresql_using="gin"),
    )

    def as_dict(self):
        return {
            "id": self.id,
            "timeseries_id": self.timeseries_id,
            "status": "ASDFASDF",
            "scheduleTime": self.scheduleTime,
            "executionTimeStart": self.executionTimeStart,
            "executionTimeEnd": self.executionTimeEnd,
            "credits": self.credits,
            "log": self.log,
            "catalog": self.catalog,
        }


class Timeseries(Base):
    __tablename__ = "timeseries"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(30))
    scenario_id = Column(Integer, ForeignKey(Scenario.id))
    description: Mapped[str]
    jobs: Mapped[List["Job"]] = relationship(back_populates="timeseries")
    catalog: Mapped[Optional[str]]

    acl_read: Mapped[[uuid]] = Column(ARRAY(UUID), nullable=False)
    __table_args__ = (
        Index(f"{__tablename__}_acl_read_index", "acl_read", postgresql_using="gin"),
    )

    def as_dict(self):
        return {
            "id": self.id,
            "scenario_id": self.scenario_id,
            "name": self.name,
            "description": self.description,
            "catalog": self.catalog,
            "jobs": [job.as_dict() for job in self.jobs],
        }


class Process(Base):
    __tablename__ = "processes"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(30))
    git_commit: Mapped[str] = mapped_column(String(40))
    git_repo: Mapped[str]
    git_location: Mapped[str]
    description: Mapped[Optional[str]]
    parameters = Column(JSONB)

    acl_read: Mapped[[uuid]] = Column(ARRAY(UUID), nullable=False)

    __table_args__ = (
        Index(f"{__tablename__}_acl_read_index", "acl_read", postgresql_using="gin"),
    )

    def as_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "git_commit": self.git_commit,
            "git_repo": self.git_repo,
            "git_location": self.git_location,
            "description": self.description,
            "parameters": self.parameters,
        }
