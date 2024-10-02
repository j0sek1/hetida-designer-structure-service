from hetdesrun.adapters.virtual_structure_adapter.config import get_vst_adapter_config
from hetdesrun.adapters.virtual_structure_adapter.utils import (
    logger,
)
from hetdesrun.structure.structure_service import (
    delete_structure,
    is_database_empty,
    load_structure_from_json_file,
    update_structure,
)


def prepopulate_structure() -> None:
    """This function handles the population of the virtual structure adapter
    with a user defined structure, if one is provided.
    """
    # Set the structure for prepopulation
    if get_vst_adapter_config().prepopulate_virtual_structure_adapter_via_file:
        structure_filepath = (
            get_vst_adapter_config().structure_filepath_to_prepopulate_virtual_structure_adapter
        )
        logger.info("Prepopulating the virtual structure adapter via a file")
        complete_structure = load_structure_from_json_file(structure_filepath)  # type: ignore
    else:
        complete_structure = (
            get_vst_adapter_config().structure_to_prepopulate_virtual_structure_adapter  # type: ignore
        )
        logger.info(
            "Prepopulating the virtual structure adapter via the environment variable "
            "'STRUCTURE_TO_PREPOPULATE_VST_ADAPTER'"
        )

    # Overwrite structure if configured
    if (
        get_vst_adapter_config().completely_overwrite_an_existing_virtual_structure_at_hd_startup
        and not is_database_empty()
    ):
        logger.info(
            "An existing structure was found in the database. The deletion process starts now"
        )
        delete_structure()
        logger.info(
            "The existing structure was successfully deleted, "
            "during the prepopulation process of the virtual structure adapter"
        )

    update_structure(complete_structure)
    logger.info("The structure was successfully populated.")
