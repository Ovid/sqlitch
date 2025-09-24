"""Target configuration for sqlitch."""

from dataclasses import dataclass
from pathlib import Path
from typing import Optional


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
        if self.uri.startswith('db:pg:'):
            return 'pg'
        elif self.uri.startswith('db:mysql:'):
            return 'mysql'
        elif self.uri.startswith('db:sqlite:'):
            return 'sqlite'
        elif self.uri.startswith('db:oracle:'):
            return 'oracle'
        elif self.uri.startswith('db:snowflake:'):
            return 'snowflake'
        elif self.uri.startswith('db:vertica:'):
            return 'vertica'
        elif self.uri.startswith('db:exasol:'):
            return 'exasol'
        elif self.uri.startswith('db:'):
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