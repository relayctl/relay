"""
models.py

Dataclasses and enums for representing Relay objects parsed from YAML specifications.
@author Ericsson Colborn
"""

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from enum import StrEnum

@dataclass
class PipelineSpec:
    """
    Represents a pipeline specification, including its name, steps, and optional metadata.
    """
    name: str
    steps: List["StepSpec"]
    description: Optional[str] = None
    version: Optional[int] = None

class StepType(StrEnum):
    """
    Enum for the type of a pipeline step.
    """
    INGEST = "ingest"
    TRANSFORM = "transform"
    CHECK = "check"
    EXPORT = "export"

@dataclass
class StepSpec:
    """
    Represents a single step in a pipeline, including its ID, type, inputs, and parameters.
    """
    id: str
    type: StepType
    inputs: List["OutputRef"]
    parameters: Dict[str, Any] = field(default_factory=dict)

@dataclass
class OutputRef:
    """
    Reference to the output of a step, used for wiring step dependencies.
    """
    step_id: str
    output: str