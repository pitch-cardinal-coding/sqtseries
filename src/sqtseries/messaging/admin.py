"""Admin command broker: REP socket serving operational commands.
Served on the admin port (12504). Commands: ``ping``, ``health``, ``stats``,
``optimize``, ``backup``, ``vacuum``, ``connections``, ``conncheck``,
``subscribers``. Heavy operations run in the handler and therefore pause
ingestion for their duration (a full VACUUM needs exclusive access anyway);
the offline CLI (``sqtseries vacuum``) is the recommended path for large
maintenance.
"""

from .broker import QueryBroker


class AdminBroker(QueryBroker):
    """REP socket for admin commands; ``handler(query_dict) -> dict``.
    Messages are ``{"cmd": "health"}``; unknown commands and validation errors

    are answered with the standard ``INVALID_REQUEST`` error shape.
    """
