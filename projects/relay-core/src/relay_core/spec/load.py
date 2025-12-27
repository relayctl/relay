"""
load.py

Functions for loading and parsing Relay pipeline specifications from YAML files into Python dataclasses.
@author Ericsson Colborn
"""

from pathlib import Path
from typing import Any, List
from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap, CommentedSeq
from relay_core.spec.models import PipelineSpec, StepSpec, StepType, OutputRef
from relay_core.spec.validate import SpecError, expect_map, expect_seq, validate_step_id, validate_inputs

_yaml = YAML(typ="rt")

def load_pipeline_spec(path: str | Path) -> PipelineSpec | None:
    """
    Load a pipeline specification from a YAML file and return a PipelineSpec object. Returns None if the file cannot be loaded.
    """
    path = Path(path)
    root = _try_open_yaml(path)
    if root is None:
        return None

    name = root.get("name")
    description = root.get("description")
    version = root.get("version")
    steps_seq = _get_steps_node(root)

    steps: List[StepSpec] = []
    seen_step_ids: set[str] = set()

    for i, step_node in enumerate(steps_seq):
        step_spec = _create_step_spec(step_node, seen_step_ids)
        if step_spec is not None:
            steps.append(step_spec)

    return PipelineSpec(
        name=name,
        description=description,
        version=version,
        steps=steps,
    )

def _try_open_yaml(path: Path) -> CommentedMap | None:
    """
    Attempt to open and parse a YAML file, returning the root mapping. Raises SpecError if parsing fails.
    """
    try:
        with path.open("r", encoding="utf-8") as f:
            root = _yaml.load(f)
            root = expect_map(root, "Top-level YAML must be a mapping (key/value object).")
            return root
    except Exception as e:
        raise SpecError(f"Failed to parse YAML: {e}") from e

def _get_steps_node(root: CommentedMap) -> CommentedSeq:
    """
    Extract and validate the 'steps' node from the root mapping. Raises SpecError if 'steps' is missing or not a sequence.
    """
    steps_node = root.get("steps")
    steps_seq = expect_seq(steps_node, "Top-level field 'steps' is required and must be a list.")
    return steps_seq

def parse_output_ref(ref_str: str) -> OutputRef:
    """
    Parse a string output reference of the form 'step_id.output_name' into an OutputRef object. Raises SpecError if the format is invalid.
    """
    if not isinstance(ref_str, str):
        raise SpecError(f"Output reference must be a string, got {type(ref_str).__name__}.")

    left, sep, right = ref_str.rpartition(".")
    if sep == "":
        raise SpecError(f"Invalid output reference '{ref_str}'. Expected 'step_id.output_name'.")

    step_id = left.strip()
    output = right.strip()
    if not step_id or not output:
        raise SpecError(f"Invalid output reference '{ref_str}'. step_id and output_name must be non-empty.")

    return OutputRef(step_id=step_id, output=output)

def _create_step_spec(step_node: Any, seen_step_ids: set[str]) -> StepSpec | None:
    """
    Create a StepSpec object from a YAML node. Raises SpecError for invalid step definitions.
    """
    step_map = expect_map(step_node, f"Each step must be a mapping/object.")

    step_id = step_map.get("id")
    validate_step_id(step_id, seen_step_ids)
    seen_step_ids.add(step_id)

    type_str = step_map.get("type")
    try:
        step_kind = StepType(type_str.strip())
    except ValueError:
        raise SpecError(f"Step {step_id}: Type '{type_str}' is invalid. Must be one of: "f"{', '.join([k.value for k in StepType])}.")

    inputs_list: List[OutputRef] = []
    validate_inputs(step_map, inputs_list, step_id)

    config_node = step_map.get("config", {})
    if config_node is None:
        config_node = {}
    if not isinstance(config_node, CommentedMap) and not isinstance(config_node, dict):
        raise SpecError(f"Step {step_id}: Config must be a mapping/object if provided.")
    parameters = dict(config_node)

    return StepSpec(
        id=step_id,
        type=step_kind,
        inputs=inputs_list,
        parameters=parameters,
    )