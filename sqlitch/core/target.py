"""Target configuration for sqlitch."""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from .config import Config


@dataclass
class Target:
    """Represents a deployment target configuration."""

    name: str
    uri: str
    engine: str = "pg"
    registry: Optional[str] = None
    client: Optional[str] = None
    top_dir: Path = Path(".")
    deploy_dir: Path = Path("deploy")
    revert_dir: Path = Path("revert")
    verify_dir: Path = Path("verify")
    plan_file: Path = Path("sqitch.plan")

    def __post_init__(self) -> None:
        """Convert string paths to Path objects."""
        if isinstance(self.top_dir, str):
            self.top_dir = Path(self.top_dir)
        if isinstance(self.deploy_dir, str):
            self.deploy_dir = Path(self.deploy_dir)
        if isinstance(self.revert_dir, str):
            self.revert_dir = Path(self.revert_dir)
        if isinstance(self.verify_dir, str):
            self.verify_dir = Path(self.verify_dir)
        if isinstance(self.plan_file, str):
            self.plan_file = Path(self.plan_file)

        # Make paths relative to top_dir if they're not absolute
        if not self.plan_file.is_absolute():
            self.plan_file = self.top_dir / self.plan_file

    @property
    def engine_type(self) -> str:
        """Extract engine type from URI or use configured engine."""
        if (
            self.uri.startswith("db:pg:")
            or self.uri.startswith("pg:")
            or self.uri.startswith("postgresql:")
        ):
            return "pg"
        elif self.uri.startswith("db:mysql:") or self.uri.startswith("mysql:"):
            return "mysql"
        elif self.uri.startswith("db:sqlite:") or self.uri.startswith("sqlite:"):
            return "sqlite"
        elif self.uri.startswith("db:oracle:") or self.uri.startswith("oracle:"):
            return "oracle"
        elif self.uri.startswith("db:snowflake:") or self.uri.startswith("snowflake:"):
            return "snowflake"
        elif self.uri.startswith("db:vertica:") or self.uri.startswith("vertica:"):
            return "vertica"
        elif self.uri.startswith("db:exasol:") or self.uri.startswith("exasol:"):
            return "exasol"
        elif self.uri.startswith("db:firebird:") or self.uri.startswith("firebird:"):
            return "firebird"
        elif self.uri.startswith("db:"):
            # URI has db: scheme but unsupported engine type
            raise ValueError(f"Unsupported engine type in URI: {self.uri}")
        else:
            # No db: scheme, use configured engine
            return self.engine

    @property
    def plan(self):
        """Get the plan for this target."""
        from .plan import Plan

        return Plan.from_file(self.plan_file)

    @classmethod
    def from_config(  # noqa: C901
        cls, config: "Config", target_name: Optional[str] = None
    ) -> "Target":
        """
        Create a Target from configuration.

        Args:
            config: Configuration object
            target_name: Target name or URI string

        Returns:
            Target instance

        Raises:
            ValueError: If target configuration is invalid
        """
        from .exceptions import hurl

        # Handle environment variable
        if not target_name:
            target_name = os.environ.get("SQITCH_TARGET", "")

        original_target_name = target_name
        uri = None
        name = None

        # If no target name, try to find default
        if not target_name:
            # Look for core.target
            target_name = config.get("core.target")

            if not target_name:
                # Look for core.engine and construct default URI
                engine = config.get("core.engine")
                if not engine:
                    if config.initialized:
                        hurl(
                            "target",
                            "No engine specified; specify via target or core.engine",
                        )
                    else:
                        hurl(
                            "target",
                            'No project configuration found. Run the "init" command to initialize a project',
                        )

                engine = engine.strip()
                # Look for engine target or use default
                target_name = config.get(f"engine.{engine}.target", f"db:{engine}:")

        # Now determine if target_name is a URI or named target
        if ":" in target_name:
            # The name is a URI
            uri = target_name
            name = None  # Name is None when it's a URI, like in Perl
        else:
            # It might be a named target or an engine name
            name = target_name
            uri = config.get(f"target.{name}.uri")

            if not uri:
                # Check if it's an engine name with engine.{name}.target
                engine_target = config.get(f"engine.{name}.target")
                if engine_target:
                    # It's an engine name, use the engine target as URI
                    uri = engine_target
                    name = None  # Name becomes None for URI-based targets
                else:
                    # Check if target section exists
                    target_section = config.get_section(f"target.{name}")
                    if not target_section:
                        hurl("target", f'Cannot find target "{name}"')
                    else:
                        hurl("target", f'No URI associated with target "{name}"')

        # Extract engine from URI
        engine = cls._extract_engine_from_uri(uri)
        if not engine:
            hurl(
                "target",
                f'No engine specified by URI {uri}; URI must start with "db:$engine:"',
            )

        # If name is None (URI case), set it to URI without password
        if name is None:
            name = uri
            # Remove password from name if present
            if "@" in uri and "://" in uri:
                try:
                    from urllib.parse import urlparse, urlunparse

                    parsed = urlparse(uri)
                    if parsed.password:
                        netloc = parsed.username
                        if parsed.hostname:
                            netloc += f"@{parsed.hostname}"
                        if parsed.port:
                            netloc += f":{parsed.port}"
                        name = urlunparse(
                            (
                                parsed.scheme,
                                netloc,
                                parsed.path,
                                parsed.params,
                                parsed.query,
                                parsed.fragment,
                            )
                        )
                except Exception:
                    pass

        # Get other configuration values - use original_target_name for config lookups if it was an engine name
        config_name = (
            original_target_name
            if original_target_name and ":" not in original_target_name
            else None
        )
        registry = cls._fetch_config_value(config, config_name, engine, "registry")
        client = cls._fetch_config_value(config, config_name, engine, "client")
        top_dir = Path(
            cls._fetch_config_value(config, config_name, engine, "top_dir") or "."
        )
        deploy_dir = Path(
            cls._fetch_config_value(config, config_name, engine, "deploy_dir")
            or "deploy"
        )
        revert_dir = Path(
            cls._fetch_config_value(config, config_name, engine, "revert_dir")
            or "revert"
        )
        verify_dir = Path(
            cls._fetch_config_value(config, config_name, engine, "verify_dir")
            or "verify"
        )
        plan_file = Path(
            cls._fetch_config_value(config, config_name, engine, "plan_file")
            or "sqitch.plan"
        )

        return cls(
            name=name,
            uri=uri,
            engine=engine,
            registry=registry,
            client=client,
            top_dir=top_dir,
            deploy_dir=deploy_dir,
            revert_dir=revert_dir,
            verify_dir=verify_dir,
            plan_file=plan_file,
        )

    @staticmethod
    def _extract_engine_from_uri(uri: str) -> Optional[str]:
        """Extract engine type from URI."""
        if uri.startswith("db:"):
            # Format: db:engine:...
            parts = uri.split(":", 2)
            if len(parts) >= 2:
                return parts[1]
        else:
            # Check for direct engine URIs like sqlite:, mysql:, etc.
            for engine in [
                "sqlite",
                "mysql",
                "pg",
                "postgresql",
                "oracle",
                "snowflake",
                "vertica",
                "exasol",
                "firebird",
            ]:
                if uri.startswith(f"{engine}:"):
                    return "pg" if engine == "postgresql" else engine
        return None

    @staticmethod
    def _fetch_config_value(
        config: "Config", target_name: Optional[str], engine: str, key: str
    ) -> Optional[str]:
        """
        Fetch configuration value with priority order:
        1. target.{name}.{key} (if target_name is provided)
        2. engine.{engine}.{key}
        3. core.{key}
        """
        # Try target-specific config (only if target_name is provided and not a URI)
        if target_name:
            value = config.get(f"target.{target_name}.{key}")
            if value:
                return value

        # Try engine-specific config
        if engine:
            value = config.get(f"engine.{engine}.{key}")
            if value:
                return value

        # Try core config
        return config.get(f"core.{key}")
