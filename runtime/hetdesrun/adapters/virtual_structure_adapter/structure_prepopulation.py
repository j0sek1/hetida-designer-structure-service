from hetdesrun.adapters.virtual_structure_adapter.config import get_vst_adapter_config
from hetdesrun.adapters.virtual_structure_adapter.utils import (
    logger,
)
from hetdesrun.structure.models import CompleteStructure
from hetdesrun.structure.structure_service import (
    delete_structure,
    is_database_empty,
    load_structure_from_json_file,
    update_structure,
)


def retrieve_complete_structure_for_prepopulation() -> CompleteStructure:
    if get_vst_adapter_config().prepopulate_virtual_structure_adapter_via_file:
        logger.info("Prepopulating the virtual structure adapter via a JSON-file")
        if (
            structure_filepath
            := get_vst_adapter_config().structure_filepath_to_prepopulate_virtual_structure_adapter
        ):
            return load_structure_from_json_file(structure_filepath)
        raise ValueError(
            "If prepopulation of the virtual structure adapter structure "
            "via a file is set, "
            "'STRUCTURE_FILEPATH_TO_PREPOPULATE_VST_ADAPTER' must be set, "
            "but it is not."
        )

    if (
        complete_structure
        := get_vst_adapter_config().structure_to_prepopulate_virtual_structure_adapter
    ):
        logger.info(
            "Prepopulating the virtual structure adapter via the environment variable "
            "'STRUCTURE_TO_PREPOPULATE_VST_ADAPTER'"
        )
        return complete_structure

    raise ValueError(
        "The prepopulation of the virtual structure adapter structure "
        "is enabled, but no structure was provided."
    )


def prepopulate_structure() -> None:
    """This function handles the population of the virtual structure adapter
    with a user defined structure, if one is provided.
    """
    logger.info("Starting the prepopulation process for the virtual structure adapter")
    if not get_vst_adapter_config().prepopulate_virtual_structure_adapter_at_designer_startup:
        logger.info(
            "Structure of the virtual structure adapter was not prepopulated, "
            "because the environment variable "
            "'PREPOPULATE_VST_ADAPTER_AT_HD_STARTUP' is set to False"
        )
        return

    complete_structure = retrieve_complete_structure_for_prepopulation()

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
    logger.info("The structure was successfully updated.")
