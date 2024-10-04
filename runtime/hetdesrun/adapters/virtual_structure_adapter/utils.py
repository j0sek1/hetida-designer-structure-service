import logging
from uuid import UUID

from hetdesrun.models.wiring import InputWiring, OutputWiring
from hetdesrun.structure.models import Sink, Source
from hetdesrun.structure.structure_service import (
    get_collection_of_sinks_from_db,
    get_collection_of_sources_from_db,
)

logger = logging.getLogger(__name__)


def get_virtual_sources_and_sinks_from_structure_service(
    input_id_list: list[str], output_id_list: list[str]
) -> tuple[dict[UUID, Source], dict[UUID, Sink]]:
    referenced_sources = get_collection_of_sources_from_db(
        [UUID(input_id) for input_id in input_id_list]
    )
    referenced_sinks = get_collection_of_sinks_from_db(
        [UUID(output_id) for output_id in output_id_list]
    )

    return referenced_sources, referenced_sinks


def get_enumerated_ids_of_vst_sources_or_sinks(
    wirings: list[InputWiring] | list[OutputWiring],
) -> tuple[list[int], list[str]]:
    indices, ref_ids = [], []
    for i, wiring in enumerate(wirings):
        if wiring.adapter_id == "virtual-structure-adapter":  # type: ignore[attr-defined]
            indices.append(i)
            ref_ids.append(wiring.ref_id)  # type: ignore[attr-defined]
    return indices, ref_ids
