from typing import Annotated
from uuid import uuid4

from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    ForeignKey,
    MetaData,
    String,
    Table,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy_utils import UUIDType

metadata = MetaData()

# Reusable types defined for cleaner and consistent ORM setup in SQLAlchemy 2.0
UUID_PK = Annotated[
    UUIDType, mapped_column(UUIDType(binary=False), primary_key=True, default=uuid4)
]
UUID_PK_FK = Annotated[
    UUIDType, mapped_column(UUIDType(binary=False), ForeignKey("element_type.id"), nullable=False)
]

OptionalUUID_PK_FK = Annotated[
    UUIDType | None,
    mapped_column(UUIDType(binary=False), ForeignKey("thing_node.id"), nullable=True),
]
Str255 = Annotated[str, mapped_column(String(255), nullable=False)]
Str255Unique = Annotated[str, mapped_column(String(255), nullable=False, unique=True)]
Str36 = Annotated[str, mapped_column(String(36), nullable=False)]
Str1024 = Annotated[str, mapped_column(String(1024), nullable=True)]
OptionalStr255 = Annotated[str | None, mapped_column(String(255), nullable=True)]
OptionalStr1024 = Annotated[str | None, mapped_column(String(1024), nullable=True)]
OptionalJSON = Annotated[dict | None, mapped_column(JSON, nullable=True)]
JSONDict = Annotated[dict, mapped_column(JSON, nullable=False)]
BoolDefaultTrue = Annotated[bool, mapped_column(Boolean, default=True)]


class Base(DeclarativeBase):
    metadata = metadata


# Association tables for many-to-many relationship
thingnode_source_association = Table(
    "thingnode_source_association",
    metadata,
    Column("thing_node_id", UUIDType(binary=False), ForeignKey("thing_node.id"), primary_key=True),
    Column("source_id", UUIDType(binary=False), ForeignKey("source.id"), primary_key=True),
)

thingnode_sink_association = Table(
    "thingnode_sink_association",
    metadata,
    Column("thing_node_id", UUIDType(binary=False), ForeignKey("thing_node.id"), primary_key=True),
    Column("sink_id", UUIDType(binary=False), ForeignKey("sink.id"), primary_key=True),
)


class ElementTypeOrm(Base):
    __tablename__ = "element_type"
    id: Mapped[UUID_PK]
    external_id: Mapped[Str255]
    stakeholder_key: Mapped[Str36]
    name: Mapped[Str255Unique]
    description: Mapped[OptionalStr1024]
    thing_nodes: Mapped[list["ThingNodeOrm"]] = relationship(
        "ThingNodeOrm", back_populates="element_type"
    )

    __table_args__ = (
        UniqueConstraint("name", name="_element_type_name_uc"),
        UniqueConstraint(
            "external_id",
            "stakeholder_key",
            name="_element_type_external_id_stakeholder_key_uc",
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<ElementTypeOrm(id={self.id}, external_id={self.external_id}, "
            f"stakeholder_key={self.stakeholder_key}, "
            f"name={self.name}, description={self.description}, "
            f"thing_nodes={[thing_node.id for thing_node in self.thing_nodes]})>"
        )


class ThingNodeOrm(Base):
    __tablename__ = "thing_node"
    id: Mapped[UUID_PK]
    external_id: Mapped[Str255]
    stakeholder_key: Mapped[Str36]
    name: Mapped[Str255Unique]
    description: Mapped[OptionalStr1024]
    parent_node_id: Mapped[OptionalUUID_PK_FK]
    parent_external_node_id: Mapped[OptionalStr255]
    element_type_id: Mapped[UUID_PK_FK]
    element_type_external_id: Mapped[Str255]
    meta_data: Mapped[OptionalJSON]
    element_type: Mapped["ElementTypeOrm"] = relationship(
        "ElementTypeOrm", back_populates="thing_nodes", uselist=False
    )

    __table_args__ = (
        UniqueConstraint("name", name="_thing_node_name_uc"),
        UniqueConstraint(
            "external_id",
            "stakeholder_key",
            name="_thing_node_external_id_stakeholder_key_uc",
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<ThingNodeOrm(id={self.id}, external_id={self.external_id}, "
            f"stakeholder_key={self.stakeholder_key}, "
            f"name={self.name}, description={self.description}, "
            f"parent_node_id={self.parent_node_id}, "
            f"parent_external_node_id={self.parent_external_node_id}, "
            f"element_type_id={self.element_type_id}, "
            f"element_type_external_id={self.element_type_external_id}, "
            f"meta_data={self.meta_data})>"
        )


class SourceOrm(Base):
    __tablename__ = "source"
    id: Mapped[UUID_PK]
    external_id: Mapped[Str255]
    stakeholder_key: Mapped[Str36]
    name: Mapped[Str255Unique]
    type: Mapped[Str255]
    visible: Mapped[BoolDefaultTrue]
    display_path: Mapped[Str255]
    adapter_key: Mapped[Str255]
    source_id: Mapped[Str255]
    ref_key: Mapped[OptionalStr1024]
    ref_id: Mapped[Str255]
    meta_data: Mapped[OptionalJSON]
    preset_filters: Mapped[JSONDict]
    passthrough_filters: Mapped[OptionalJSON]
    thing_node_external_ids: Mapped[OptionalJSON]

    thing_nodes: Mapped[list["ThingNodeOrm"]] = relationship(
        "ThingNodeOrm", secondary=thingnode_source_association
    )

    __table_args__ = (
        UniqueConstraint(
            "external_id",
            "stakeholder_key",
            name="_source_external_id_stakeholder_key_uc",
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<SourceOrm(id={self.id}, external_id={self.external_id}, "
            f"stakeholder_key={self.stakeholder_key}, "
            f"name={self.name}, type={self.type}, visible={self.visible}, "
            f"display_path={self.display_path}, "
            f"adapter_key={self.adapter_key}, "
            f"source_id={self.source_id}, meta_data={self.meta_data}, "
            f"preset_filters={self.preset_filters}, "
            f"passthrough_filters={self.passthrough_filters}, "
            f"thing_nodes={[thing_node.id for thing_node in self.thing_nodes]})>"
        )


class SinkOrm(Base):
    __tablename__ = "sink"

    id: Mapped[UUID_PK]
    external_id: Mapped[Str255]
    stakeholder_key: Mapped[Str36]
    name: Mapped[Str255Unique]
    type: Mapped[Str255]
    visible: Mapped[BoolDefaultTrue]
    display_path: Mapped[Str255]
    adapter_key: Mapped[Str255]
    sink_id: Mapped[Str255]
    ref_key: Mapped[OptionalStr1024]
    ref_id: Mapped[Str255]
    meta_data: Mapped[OptionalJSON]
    preset_filters: Mapped[JSONDict]
    passthrough_filters: Mapped[OptionalJSON]
    thing_node_external_ids: Mapped[OptionalJSON]

    thing_nodes: Mapped[list["ThingNodeOrm"]] = relationship(
        "ThingNodeOrm", secondary=thingnode_sink_association
    )

    __table_args__ = (
        UniqueConstraint(
            "external_id",
            "stakeholder_key",
            name="_sink_external_id_stakeholder_key_uc",
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<SinkOrm(id={self.id}, external_id={self.external_id}, "
            f"stakeholder_key={self.stakeholder_key}, "
            f"name={self.name}, type={self.type}, visible={self.visible}, "
            f"display_path={self.display_path}, "
            f"adapter_key={self.adapter_key}, "
            f"sink_id={self.sink_id}, meta_data={self.meta_data}, "
            f"preset_filters={self.preset_filters}, "
            f"passthrough_filters={self.passthrough_filters}, "
            f"thing_nodes={[thing_node.id for thing_node in self.thing_nodes]})>"
        )
