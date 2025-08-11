import click

from chancy import Chancy
from chancy.cli import run_async_command
from chancy.rate_limit import RateLimit


@click.group(name="rate-limit")
def rate_limit_group():
    """
    Rate limit management commands.
    """
    pass


@rate_limit_group.command("declare")
@click.argument("rate_limit_key")
@click.argument("rate_limit", type=int)
@click.argument("rate_limit_window", type=int)
@click.option(
    "--upsert",
    "-u",
    is_flag=True,
    help="Update the rate limit configuration if it already exists.",
)
@click.pass_context
@run_async_command
async def declare_rate_limit(
    ctx: click.Context,
    rate_limit_key: str,
    rate_limit: int,
    rate_limit_window: int,
    upsert: bool,
):
    """
    Declare a global rate limit configuration
    or update an existing one.
    """
    chancy: Chancy = ctx.obj["app"]

    async with chancy:
        await chancy.declare_rate_limit(
            RateLimit(
                rate_limit_key=rate_limit_key,
                rate_limit=rate_limit,
                rate_limit_window=rate_limit_window,
            ),
            upsert=upsert,
        )


@rate_limit_group.command("delete")
@click.argument("rate_limit_key")
@click.pass_context
@run_async_command
async def delete_rate_limit(ctx: click.Context, rate_limit_key: str):
    """
    Delete a rate limit configuration.
    """
    chancy: Chancy = ctx.obj["app"]

    if click.confirm(
        f"Are you sure you want to delete rate limit '{rate_limit_key}'?"
    ):
        async with chancy:
            await chancy.delete_rate_limit(rate_limit_key)
