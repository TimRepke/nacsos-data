class NotFoundError(Exception):
    """
    This error is raised when no data was found for a specified query. Note, that not all
    methods use this error and might rather return `None` or `[]`, depending on the use-case.
    """
    pass


class InvalidFilterError(AssertionError):
    """
    Used in `nacsos_data.util.annotations.resolve` to indicate that the configuration
    for resolving annotations is not valid.
    """
    pass


class EmptyAnnotationsError(ValueError):
    """
    Used in `nacsos_data.util.annotations.resolve` to indicate that collected
    user annotations for a label are not valid. Usually this is caused by missing
    annotations for that label.
    """
    pass
