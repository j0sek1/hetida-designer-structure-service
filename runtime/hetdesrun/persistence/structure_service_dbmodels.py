from uuid import uuid4

from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    ForeignKey,
    Index,
    String,
    Table,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, declarative_base, relationship
from sqlalchemy_utils import UUIDType

Base = declarative_base()

# Association table between ThingNode and Source
thingnode_source_association = Table(
    "structure_thingnode_source_association",
    Base.metadata,
    Column(
        "thingnode_id",
        UUIDType(binary=False),
        ForeignKey("structure_thing_node.id"),
        primary_key=True,
    ),
    Column(
        "source_id", UUIDType(binary=False), ForeignKey("structure_source.id"), primary_key=True
    ),
)

# Association table between ThingNode and Sink
thingnode_sink_association = Table(
    "structure_thingnode_sink_association",
    Base.metadata,
    Column(
        "thingnode_id",
        UUIDType(binary=False),
        ForeignKey("structure_thing_node.id"),
        primary_key=True,
    ),
    Column("sink_id", UUIDType(binary=False), ForeignKey("structure_sink.id"), primary_key=True),
)


# ORM model for ElementType
class StructureServiceElementTypeDBModel(Base):
    __tablename__ = "structure_element_type"
    id: UUIDType = Column(
        UUIDType(binary=False),
        primary_key=True,  # Primary key for unique identification
        nullable=False,
        default=uuid4,
    )
    external_id = Column(String(255), nullable=False)
    stakeholder_key = Column(String(36), nullable=False)
    name = Column(String(255), index=True, nullable=False, unique=True)
    description = Column(String(1024), nullable=True)
    thing_nodes: list["StructureServiceThingNodeDBModel"] = relationship(
        "StructureServiceThingNodeDBModel", back_populates="element_type"
    )

    # Constraints and Indexes for optimized search and uniqueness
    __table_args__ = (
        UniqueConstraint("name", name="_element_type_name_uc"),  # Enforces unique names
        UniqueConstraint(
            "external_id",
            "stakeholder_key",
            name="_element_type_external_id_stakeholder_key_uc",
        ),
        Index(
            "idx_element_type_stakeholder_external",  # Optimized search on stakeholder_key and external_id  # noqa: E501
            "stakeholder_key",
            "external_id",
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<StructureServiceElementTypeDBModel(id={self.id}, external_id={self.external_id}, "
            f"stakeholder_key={self.stakeholder_key}, "
            f"name={self.name}, description={self.description}, "
            f"thing_nodes={[thing_node.id for thing_node in self.thing_nodes]})>"
        )


# ORM model for Source
class StructureServiceSourceDBModel(Base):
    __tablename__ = "structure_source"
    id: UUIDType = Column(UUIDType(binary=False), primary_key=True, default=uuid4)
    external_id = Column(String(255), nullable=False)
    stakeholder_key = Column(String(36), nullable=False)
    name: str = Column(String(255), nullable=False, unique=True)
    type: str = Column(String(255), nullable=False)
    visible: bool = Column(Boolean, default=True)
    display_path: str = Column(String(255), nullable=False)
    adapter_key: str = Column(String(255), nullable=False)
    source_id: str = Column(String(255), nullable=False)
    ref_key: str | None = Column(String(255), nullable=True)
    ref_id: str = Column(String(255), nullable=False)
    meta_data: dict | None = Column(JSON, nullable=True)
    preset_filters: dict = Column(JSON, nullable=False)
    passthrough_filters: list[dict] | None = Column(JSON, nullable=True)
    thing_node_external_ids: list[str] = Column(JSON, nullable=True)

    # Defines Many-to-Many relationship with StructureServiceThingNodeDBModel
    thing_nodes: list["StructureServiceThingNodeDBModel"] = relationship(
        "StructureServiceThingNodeDBModel",
        # Association table for Many-to-Many relation:
        secondary=thingnode_source_association,
        back_populates="sources",
        # 'back_populates' specifies reciprocal relationship in
        # StructureServiceThingNodeDBModel
    )  # type: ignore

    __table_args__ = (
        UniqueConstraint(
            "external_id",
            "stakeholder_key",
            name="_source_external_id_stakeholder_key_uc",
        ),
        Index(
            "idx_source_stakeholder_external",
            "stakeholder_key",
            "external_id",
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<StructureServiceSourceDBModel(id={self.id}, external_id={self.external_id}, "
            f"stakeholder_key={self.stakeholder_key}, "
            f"name={self.name}, type={self.type}, visible={self.visible}, "
            f"display_path={self.display_path}, "
            f"adapter_key={self.adapter_key}, "
            f"source_id={self.source_id}, meta_data={self.meta_data}, "
            f"preset_filters={self.preset_filters}, "
            f"passthrough_filters={self.passthrough_filters}, "
            f"thing_nodes={[thing_node.id for thing_node in self.thing_nodes]})>"
        )


# ORM model for Sink
class StructureServiceSinkDBModel(Base):
    __tablename__ = "structure_sink"
    id: UUIDType = Column(UUIDType(binary=False), primary_key=True, default=uuid4)
    external_id = Column(String(255), nullable=False)
    stakeholder_key = Column(String(36), nullable=False)
    name: str = Column(String(255), nullable=False, unique=True)
    type: str = Column(String(255), nullable=False)
    visible: bool = Column(Boolean, default=True)
    display_path: str = Column(String(255), nullable=False)
    adapter_key: str = Column(String(255), nullable=False)
    sink_id: str = Column(String(255), nullable=False)
    ref_key: str | None = Column(String(255), nullable=True)
    ref_id: str = Column(String(255), nullable=False)
    meta_data: dict | None = Column(JSON, nullable=True)
    preset_filters: dict = Column(JSON, nullable=False)
    passthrough_filters: list[dict] | None = Column(JSON, nullable=True)
    thing_node_external_ids: list[str] = Column(JSON, nullable=True)

    # Defines Many-to-Many relationship with StructureServiceThingNodeDBModel
    thing_nodes: list["StructureServiceThingNodeDBModel"] = relationship(
        "StructureServiceThingNodeDBModel",
        # Association table for Many-to-Many relation:
        secondary=thingnode_sink_association,
        back_populates="sinks",
        # 'back_populates' specifies reciprocal relationship in
        # StructureServiceThingNodeDBModel
    )  # type: ignore

    __table_args__ = (
        UniqueConstraint(
            "external_id",
            "stakeholder_key",
            name="_sink_external_id_stakeholder_key_uc",
        ),
        Index(
            "idx_sink_stakeholder_external",
            "stakeholder_key",
            "external_id",
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<StructureServiceSinkDBModel(id={self.id}, external_id={self.external_id}, "
            f"stakeholder_key={self.stakeholder_key}, "
            f"name={self.name}, type={self.type}, visible={self.visible}, "
            f"display_path={self.display_path}, "
            f"adapter_key={self.adapter_key}, "
            f"sink_id={self.sink_id}, meta_data={self.meta_data}, "
            f"preset_filters={self.preset_filters}, "
            f"passthrough_filters={self.passthrough_filters}, "
            f"thing_nodes={[thing_node.id for thing_node in self.thing_nodes]})>"
        )


# ORM model for ThingNode
class StructureServiceThingNodeDBModel(Base):
    __tablename__ = "structure_thing_node"
    id: UUIDType = Column(UUIDType(binary=False), primary_key=True, default=uuid4)
    external_id = Column(String(255), nullable=False)
    stakeholder_key = Column(String(36), nullable=False)
    name = Column(String(255), index=True, nullable=False, unique=True)
    description = Column(String(1024), nullable=True)
    parent_node_id: UUIDType = Column(
        UUIDType(binary=False), ForeignKey("structure_thing_node.id"), nullable=True
    )
    parent_external_node_id = Column(String(255), nullable=True)
    element_type_id: UUIDType = Column(
        UUIDType(binary=False),
        ForeignKey("structure_element_type.id"),
        nullable=False,
    )
    element_type_external_id = Column(String(255), nullable=False)
    meta_data = Column(JSON, nullable=True)
    element_type: Mapped["StructureServiceElementTypeDBModel"] = relationship(
        "StructureServiceElementTypeDBModel", back_populates="thing_nodes", uselist=False
    )

    # Defines Many-to-Many relationship with StructureServiceSourceDBModel
    sources: list["StructureServiceSourceDBModel"] = relationship(
        "StructureServiceSourceDBModel",
        # Association table for Many-to-Many relation:
        secondary=thingnode_source_association,
        back_populates="thing_nodes",
        # 'back_populates' specifies reciprocal relationship in
        # StructureServiceSourceDBModel
    )

    # Defines Many-to-Many relationship with StructureServiceSinkDBModel
    sinks: list["StructureServiceSinkDBModel"] = relationship(
        "StructureServiceSinkDBModel",
        # Association table for Many-to-Many relation:
        secondary=thingnode_sink_association,
        back_populates="thing_nodes",
        # 'back_populates' specifies reciprocal relationship in
        # StructureServiceSinkDBModel
    )

    # Constraints and Indexes for optimized search and uniqueness
    __table_args__ = (
        UniqueConstraint("name", name="_thing_node_name_uc"),  # Enforces unique names
        UniqueConstraint(
            "external_id",
            "stakeholder_key",
            name="_thing_node_external_id_stakeholder_key_uc",
        ),
        Index(
            "idx_thing_node_stakeholder_external",  # Optimized search on stakeholder_key and external_id  # noqa: E501
            "stakeholder_key",
            "external_id",
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<StructureServiceThingNodeDBModel(id={self.id}, external_id={self.external_id}, "
            f"stakeholder_key={self.stakeholder_key}, "
            f"name={self.name}, description={self.description}, "
            f"parent_node_id={self.parent_node_id}, "
            f"parent_external_node_id={self.parent_external_node_id}, "
            f"element_type_id={self.element_type_id}, "
            f"element_type_external_id={self.element_type_external_id}, "
            f"meta_data={self.meta_data})>"
        )
