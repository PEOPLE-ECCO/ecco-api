import datetime
import enum
import uuid
from typing import Optional

import sqlalchemy
from geoalchemy2 import Geography
from sqlalchemy import Column, String, Text, func, Index, Integer, Sequence, ForeignKey, Identity
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import Mapped, DeclarativeBase, mapped_column, column_property
from sqlalchemy.sql.type_api import UserDefinedType


class Base(DeclarativeBase):
    ...


class Box2D(UserDefinedType):
    def get_col_spec(self, **kw):
        return "box2d"

    def bind_processor(self, dialect):
        def process(value):
            # value expected as: 'minx miny, maxx maxy'
            return f'BOX({value})'

        return process

    def result_processor(self, dialect, coltype):
        def process(value):
            ll, ur = value[4:-1].split(",")
            min_x, min_y = ll.split(" ")
            max_x, max_y =ur.split(" ")
            return [float(min_x), float(min_y), float(max_x), float(max_y)]

        return process


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = Column(Integer, Sequence('seq_user_id', start=10, increment=1), primary_key=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
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
    name: Mapped[str] = mapped_column(String(100))
    bbox = mapped_column(Box2D)
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
            "bbox": self.bbox,
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
    flow_run_name: Mapped[str] = mapped_column(String(50))
    timeseries_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("timeseries.id"))
    scheduleTime: Mapped[datetime.datetime] = mapped_column(server_default=func.now())

    user_id = Column(Integer, ForeignKey(User.id))
    acl_read: Mapped[[uuid]] = Column(ARRAY(sqlalchemy.types.UUID), nullable=False)

    __table_args__ = (
        Index(f"{__tablename__}_acl_read_index", "acl_read", postgresql_using="gin"),
    )

    def as_dict(self):
        return {
            "id": self.id,
            "timeseries_id": self.timeseries_id,
        }


class Timeseries(Base):
    __tablename__ = "timeseries"

    id: Mapped[int] = mapped_column(primary_key=True, server_default=Identity())
    name: Mapped[str] = mapped_column(String(100))
    bbox = mapped_column(Box2D)
    geometry = Column(Geography(geometry_type='POLYGON', srid=4326))
    scenario_id = Column(Integer, ForeignKey(Scenario.id))
    description: Mapped[str]

    process = Column(Integer, ForeignKey("processes.id"))
    process_parameters: Mapped[Optional[dict]] = Column(JSONB)
    geom_geojson = column_property(
        func.ST_AsGeoJSON(geometry)
    )

    acl_read: Mapped[[uuid.UUID]] = Column(ARRAY(sqlalchemy.types.UUID), nullable=False)
    __table_args__ = (
        Index(f"{__tablename__}_acl_read_index", "acl_read", postgresql_using="gin"),
    )

    def as_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "scenario_id": self.scenario_id,
            "description": self.description,
            "geometry": self.geom_geojson,
            "bbox": self.bbox,
            "process": self.process,
            "process_parameters": self.process_parameters,
        }


class Process(Base):
    __tablename__ = "processes"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(100))
    deployment_id: Mapped[uuid.UUID] = mapped_column(sqlalchemy.types.UUID)
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
            "description": self.description,
        }
