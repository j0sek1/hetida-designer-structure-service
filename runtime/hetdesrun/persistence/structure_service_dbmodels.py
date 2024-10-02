from uuid import uuid4

from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    PrimaryKeyConstraint,
    String,
    Table,
    UniqueConstraint,
    and_,
)
from sqlalchemy.orm import Mapped, declarative_base, relationship
from sqlalchemy_utils import UUIDType

Base = declarative_base()

# Association table between ThingNode and Source
thingnode_source_association = Table(
    "thingnode_source_association",
    Base.metadata,
    # Columns for ThingNode and Source relationship
    Column("thing_node_stakeholder_key", String(36), nullable=False),
    Column("thing_node_external_id", String(255), nullable=False),
    Column("source_stakeholder_key", String(36), nullable=False),
    Column("source_external_id", String(255), nullable=False),
    # Foreign key constraints to enforce referential integrity
    ForeignKeyConstraint(
        ["thing_node_stakeholder_key", "thing_node_external_id"],
        ["thing_node.stakeholder_key", "thing_node.external_id"],
    ),
    ForeignKeyConstraint(
        ["source_stakeholder_key", "source_external_id"],
        ["source.stakeholder_key", "source.external_id"],
    ),
    # Composite primary key for uniqueness in association
    PrimaryKeyConstraint(
        "thing_node_stakeholder_key",
        "thing_node_external_id",
        "source_stakeholder_key",
        "source_external_id",
    ),
)

# Association table between ThingNode and Sink
thingnode_sink_association = Table(
    "thingnode_sink_association",
    Base.metadata,
    # Columns for ThingNode and Sink relationship
    Column("thing_node_stakeholder_key", String(36), nullable=False),
    Column("thing_node_external_id", String(255), nullable=False),
    Column("sink_stakeholder_key", String(36), nullable=False),
    Column("sink_external_id", String(255), nullable=False),
    # Foreign key constraints to enforce referential integrity
    ForeignKeyConstraint(
        ["thing_node_stakeholder_key", "thing_node_external_id"],
        ["thing_node.stakeholder_key", "thing_node.external_id"],
    ),
    ForeignKeyConstraint(
        ["sink_stakeholder_key", "sink_external_id"],
        ["sink.stakeholder_key", "sink.external_id"],
    ),
    # Composite primary key for uniqueness in association
    PrimaryKeyConstraint(
        "thing_node_stakeholder_key",
        "thing_node_external_id",
        "sink_stakeholder_key",
        "sink_external_id",
    ),
)


# ORM model for ElementType
class ElementTypeOrm(Base):
    __tablename__ = "element_type"
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
    thing_nodes: list["ThingNodeOrm"] = relationship("ThingNodeOrm", back_populates="element_type")

    # Constraints and Indexes for optimized search and uniqueness
    __table_args__ = (
        UniqueConstraint("name", name="_element_type_name_uc"),  # Enforces unique names
        UniqueConstraint(
            "external_id",
            "stakeholder_key",
            name="_element_type_external_id_stakeholder_key_uc",
        ),
        Index(
            "idx_element_type_stakeholder_external",  # Optimized search on stakeholder_key and external_id
            "stakeholder_key",
            "external_id",
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<ElementTypeOrm(id={self.id}, external_id={self.external_id}, "
            f"stakeholder_key={self.stakeholder_key}, "
            f"name={self.name}, description={self.description}, "
            f"thing_nodes={[thing_node.id for thing_node in self.thing_nodes]})>"
        )


# ORM model for Source
class SourceOrm(Base):
    __tablename__ = "source"
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

    # Defines Many-to-Many relationship with ThingNodeOrm
    # Using lambda for lazy evaluation, allowing resolution of join conditions at runtime
    # to handle circular dependencies between ThingNodeOrm and SinkOrm.
    thing_nodes: list["ThingNodeOrm"] = relationship(
        "ThingNodeOrm",
        secondary=thingnode_source_association,  # Association table for Many-to-Many relation
        primaryjoin=lambda: and_(
            SourceOrm.stakeholder_key == thingnode_source_association.c.source_stakeholder_key,
            SourceOrm.external_id == thingnode_source_association.c.source_external_id,
        ),
        secondaryjoin=lambda: and_(
            ThingNodeOrm.stakeholder_key
            == thingnode_source_association.c.thing_node_stakeholder_key,
            ThingNodeOrm.external_id == thingnode_source_association.c.thing_node_external_id,
        ),
        back_populates="sources",  # Specifies reciprocal relationship in ThingNodeOrm
    )

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


# ORM model for Sink
class SinkOrm(Base):
    __tablename__ = "sink"
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

    # Defines Many-to-Many relationship with ThingNodeOrm
    # Using lambda for lazy evaluation, allowing resolution of join conditions at runtime
    # to handle circular dependencies between ThingNodeOrm and SinkOrm.
    thing_nodes: list["ThingNodeOrm"] = relationship(
        "ThingNodeOrm",
        secondary=thingnode_sink_association,  # Association table for Many-to-Many relation
        primaryjoin=lambda: and_(
            SinkOrm.stakeholder_key == thingnode_sink_association.c.sink_stakeholder_key,
            SinkOrm.external_id == thingnode_sink_association.c.sink_external_id,
        ),
        secondaryjoin=lambda: and_(
            ThingNodeOrm.stakeholder_key == thingnode_sink_association.c.thing_node_stakeholder_key,
            ThingNodeOrm.external_id == thingnode_sink_association.c.thing_node_external_id,
        ),
        back_populates="sinks",  # Specifies reciprocal relationship in ThingNodeOrm
    )

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


# ORM model for ThingNode
class ThingNodeOrm(Base):
    __tablename__ = "thing_node"
    id: UUIDType = Column(UUIDType(binary=False), primary_key=True, default=uuid4)
    external_id = Column(String(255), nullable=False)
    stakeholder_key = Column(String(36), nullable=False)
    name = Column(String(255), index=True, nullable=False, unique=True)
    description = Column(String(1024), nullable=True)
    parent_node_id: UUIDType = Column(
        UUIDType(binary=False), ForeignKey("thing_node.id"), nullable=True
    )
    parent_external_node_id = Column(String(255), nullable=True)
    element_type_id: UUIDType = Column(
        UUIDType(binary=False),
        ForeignKey("element_type.id"),
        nullable=False,
    )
    element_type_external_id = Column(String(255), nullable=False)
    meta_data = Column(JSON, nullable=True)
    element_type: Mapped["ElementTypeOrm"] = relationship(
        "ElementTypeOrm", back_populates="thing_nodes", uselist=False
    )

    # Defines Many-to-Many relationship with SourceOrm
    # Using lambda for lazy evaluation, allowing resolution of join conditions at runtime
    # to handle circular dependencies between ThingNodeOrm and SinkOrm.
    sources: list["SourceOrm"] = relationship(
        "SourceOrm",
        secondary=thingnode_source_association,  # Association table for Many-to-Many relation
        primaryjoin=lambda: and_(
            ThingNodeOrm.stakeholder_key
            == thingnode_source_association.c.thing_node_stakeholder_key,
            ThingNodeOrm.external_id == thingnode_source_association.c.thing_node_external_id,
        ),
        secondaryjoin=lambda: and_(
            SourceOrm.stakeholder_key == thingnode_source_association.c.source_stakeholder_key,
            SourceOrm.external_id == thingnode_source_association.c.source_external_id,
        ),
        back_populates="thing_nodes",  # Specifies reciprocal relationship in SourceOrm
    )

    # Defines Many-to-Many relationship with SinkOrm
    # Using lambda for lazy evaluation, allowing resolution of join conditions at runtime
    # to handle circular dependencies between ThingNodeOrm and SinkOrm.
    sinks: list["SinkOrm"] = relationship(
        "SinkOrm",
        secondary=thingnode_sink_association,  # Association table for Many-to-Many relation
        primaryjoin=lambda: and_(
            ThingNodeOrm.stakeholder_key == thingnode_sink_association.c.thing_node_stakeholder_key,
            ThingNodeOrm.external_id == thingnode_sink_association.c.thing_node_external_id,
        ),
        secondaryjoin=lambda: and_(
            SinkOrm.stakeholder_key == thingnode_sink_association.c.sink_stakeholder_key,
            SinkOrm.external_id == thingnode_sink_association.c.sink_external_id,
        ),
        back_populates="thing_nodes",  # Specifies reciprocal relationship in SinkOrm
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
            "idx_thing_node_stakeholder_external",  # Optimized search on stakeholder_key and external_id
            "stakeholder_key",
            "external_id",
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
