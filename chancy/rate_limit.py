import dataclasses
from dataclasses import KW_ONLY


@dataclasses.dataclass(frozen=True)
class RateLimit:
    """
    Rate limits are used to control the rate at which jobs are processed
    across queues and workers. They can be shared between queues and jobs
    by using the same rate_limit_key.

    Rate limits must be declared using :func:`~chancy.app.Chancy.declare_rate_limit`
    before they can be used by queues or jobs.

    .. code-block:: python

        async with Chancy("postgresql://localhost/postgres") as chancy:
            await chancy.declare_rate_limit(
                RateLimit(
                    rate_limit_key="amazon_api",
                    rate_limit=30,
                    rate_limit_window=60
                )
            )

    Once declared, rate limits can be used by queues:

    .. code-block:: python

        await chancy.declare(
            Queue(name="api_queue", rate_limit_key="amazon_api")
        )

    Or by individual jobs:

    .. code-block:: python

        job = Job("api_call").with_rate_limit("amazon_api")
        await chancy.push(job)

    Rate limits can also use partition keys to apply limits per partition:

    .. code-block:: python

        job = Job("api_call").with_rate_limit("amazon_api").with_rate_limit_partition("customer_123")
    """

    #: A globally unique identifier for the rate limit configuration
    rate_limit_key: str

    _ = KW_ONLY
    #: The maximum number of operations allowed per time window
    rate_limit: int
    #: The time window in seconds over which the rate limit applies
    rate_limit_window: int

    @classmethod
    def unpack(cls, data: dict) -> "RateLimit":
        """
        Unpack a serialized rate limit object into a RateLimit instance.
        """
        return cls(
            rate_limit_key=data["rate_limit_key"],
            rate_limit=data["rate_limit"],
            rate_limit_window=data["rate_limit_window"],
        )

    def pack(self) -> dict:
        """
        Pack the rate limit into a dictionary that can be serialized and used to
        recreate the rate limit later.
        """
        return {
            "rate_limit_key": self.rate_limit_key,
            "rate_limit": self.rate_limit,
            "rate_limit_window": self.rate_limit_window,
        }
