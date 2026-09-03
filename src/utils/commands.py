"""Helpers for applying the configured suffix to every Telegram command."""

from pyrogram import filters


def command_filter(config, names: list[str]):
    """Build a Pyrogram command filter with the configured numeric postfix."""
    return filters.command([f"{name}{config.command_postfix}" for name in names])


def command_text(config, name: str) -> str:
    """Return a display-ready command, for example ``/rename2``."""
    return f"/{name}{config.command_postfix}"
