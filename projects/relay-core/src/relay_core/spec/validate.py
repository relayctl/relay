"""
validate.py

Validation utilities for Relay pipeline specifications.
@author Ericsson Colborn
"""

from typing import Any, List
from ruamel.yaml.comments import CommentedMap, CommentedSeq
from relay_core.spec.models import OutputRef
from relay_core.spec.load import parse_output_ref

class SpecError(ValueError):
    """
    Exception raised for user-facing specification errors.
    """
    pass

def expect_map(node: Any, msg: str) -> CommentedMap:
    """
    Ensure the given node is a mapping (CommentedMap). Raise SpecError with the provided message if not.
    """
    if not isinstance(node, CommentedMap):
        raise SpecError(msg)
    return node

def expect_seq(node: Any, msg: str) -> CommentedSeq:
    """
    Ensure the given node is a sequence (CommentedSeq). Raise SpecError with the provided message if not.
    """
    if not isinstance(node, CommentedSeq):
        raise SpecError(msg)
    return node

def validate_step_id(step_id: Any, seen_step_ids: set) -> bool:
    """
    Validate that the step ID is a non-empty string and not a duplicate. Raise SpecError if invalid.
    """
    if not isinstance(step_id, str) or not step_id.strip():
        raise SpecError(f"Step ID is required and must be a non-empty string.")
    if step_id in seen_step_ids:
        raise SpecError(f"Duplicate step ID '{step_id}'.")
    return True

def validate_inputs(step_map: CommentedMap, inputs_list: List[OutputRef], step_id: str) -> bool:
    """
    Validate and parse the 'inputs' field of a step, appending OutputRef objects to inputs_list. Raise SpecError if any input reference is invalid.
    """
    inputs_node = step_map.get("inputs")
    if inputs_node is not None:
        inputs_map = expect_map(inputs_node, f"Step {step_id}: Inputs must be mappings/objects if provided.")
        for input_name, ref in inputs_map.items():
            try:
                inputs_list.append(parse_output_ref(ref))
            except SpecError as e:
                raise SpecError(f"Step {step_id}, input '{input_name}': {e}") from e
    return True
