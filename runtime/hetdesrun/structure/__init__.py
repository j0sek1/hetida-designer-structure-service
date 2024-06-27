import logging

from hetdesrun.structure.config import get_config


def configure_logging(
    the_logger: logging.Logger,
) -> None:
    """Configure logging

    Arguments:
        the_logger {Python logger} -- any logger

    If log_execution_context is True a LoggingFilter will be attached to the
    LogHandler. Attaching to the handler (instead of the logger) guarantees that
    the filter will be applied even when descendant loggers are used which don't have
    handlers on their own (confer https://docs.python.org/3/_images/logging_flow.png)

    This filter actually does no filtering but augments the LogRecords with
    execution context information (id of component instance and component uuid).
    A filter is used here for context provision because it can be attached to
    a handler (in contrast to a LoggingAdapter). Attaching the filter to custom
    LoggingHandlers allows to send this information to external services.

    Additionally the formatter is set up to log this context information.
    """
    the_logger.setLevel(get_config().log_level.value)
    logging_handler = logging.StreamHandler()  # use sys.stderr by default
    # sys.stderr will be propagated by mod_wsgi to Apache error log for webservice

    formatter = logging.Formatter(
        "%(asctime)s %(process)d %(levelname)s: %(message)s "
        "[in %(pathname)s:%(lineno)d]"
    )
    logging_handler.setFormatter(formatter)
    the_logger.addHandler(logging_handler)


main_logger = logging.getLogger(__name__)
configure_logging(main_logger)

main_logger.info("Logging setup complete.")