"""Target configuration for sqitch."""

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
    top_dir: Path = Path(".")
    deploy_dir: Path = Path("deploy")
    revert_dir: Path = Path("revert")
    verify_dir: Path = Path("verify")
    
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