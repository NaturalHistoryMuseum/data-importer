from functools import partial
from functools import partial
from pathlib import Path
from typing import Optional, Any

import click
from click import Parameter, Context
from rich.console import Console

from dataimporter.lib.config import Config, load, ConfigLoadError

# environment variable name for config path setting
CONFIG_ENV_VAR = "DIMP_CONFIG"

# global console for all to use
console: Console = Console()


class ConfigType(click.Path):
    """
    Click type allowing CLI functions to get a config object from a path.
    """

    name = "config"

    def __init__(self):
        super().__init__(
            exists=True, file_okay=True, dir_okay=False, readable=True, path_type=Path
        )

    def convert(
        self, value: Any, param: Optional[Parameter], ctx: Optional[Context]
    ) -> Config:
        """
        Convert the given value to a Config object.

        :param value: the value passed from Click, hopefully this is a path of some kind
        :param param: the parameter that is using this type to convert its value. May be
                      None.
        :param ctx: the current context that arrived at this value. May be None.
        :return: a config object
        """
        if isinstance(value, Config):
            return value

        path: Path = Path(super().convert(value, param, ctx))
        try:
            return load(path)
        except ConfigLoadError as e:
            self.fail(
                f"Failed to load config from {path} due to {e.reason}",
                param,
                ctx,
            )
        except Exception as e:
            self.fail(
                f"Failed to load config from {path} due to {str(e)}",
                param,
                ctx,
            )


# decorator which adds the config click arg to any click command function
with_config = partial(
    click.argument, "config", type=ConfigType(), envvar=CONFIG_ENV_VAR
)
