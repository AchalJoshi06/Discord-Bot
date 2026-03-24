"""Structured logging setup with RotatingFileHandler."""
import logging
import sys
from logging.handlers import RotatingFileHandler


def setup_logging(level: int = logging.INFO) -> None:
    """Configure structured logging for the bot.

    Creates two handlers:
      1. Console (stdout) — coloured by level
      2. File (bot.log) — max 5 MB, 3 backups
    """
    root = logging.getLogger("cc2bot")
    root.setLevel(level)

    # Prevent duplicate handlers on reload
    if root.handlers:
        return

    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)-25s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # ── Console handler ──
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(level)
    console.setFormatter(fmt)
    root.addHandler(console)

    # ── File handler (rotating 5 MB × 3) ──
    try:
        file_handler = RotatingFileHandler(
            "bot.log", maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(fmt)
        root.addHandler(file_handler)
    except Exception as e:
        root.warning(f"Could not create file log handler: {e}")

    # Quiet noisy third-party loggers
    logging.getLogger("discord").setLevel(logging.WARNING)
    logging.getLogger("discord.http").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
