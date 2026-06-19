class SlackForbiddenError(Exception):
    """
    Raised when the Slack API returns an error indicating the credentials
    do not have permission to access a resource (e.g., missing_scope).
    """
    pass
