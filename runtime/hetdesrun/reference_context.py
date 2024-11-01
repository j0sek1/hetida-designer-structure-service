from contextvars import ContextVar
from copy import deepcopy

from hetdesrun.models.repr_reference import (
    ReproducibilityReference,
    StructureServiceMetadataReference,
)

reproducibility_reference_context: ContextVar[ReproducibilityReference] = ContextVar(
    "reproducibility_reference_context"
)
structure_service_metadata_reference_context: ContextVar[StructureServiceMetadataReference] = (
    ContextVar("structure_service_metadata_reference_context")
)


def get_reproducibility_reference_context() -> ReproducibilityReference:
    try:
        return reproducibility_reference_context.get()
    except LookupError:
        reproducibility_reference_context.set(ReproducibilityReference())
        return reproducibility_reference_context.get()


def get_deepcopy_of_reproducibility_reference_context() -> ReproducibilityReference:
    return deepcopy(get_reproducibility_reference_context())


def set_reproducibility_reference_context(
    new_reference: ReproducibilityReference,
) -> None:
    reproducibility_reference_context.set(new_reference)


# TODO Add getter, setter and maybe deepcopy for metadata reference
