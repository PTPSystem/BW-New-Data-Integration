from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import yaml


@dataclass(frozen=True)
class PipelineDefinition:
    name: str
    mdx: str
    parser: str
    mapping_path: str
    catalog: Optional[str] = None
    hierarchy_mappings: Optional[List[Dict[str, str]]] = None


def render_mdx_template(mdx: str, variables: Dict[str, Any] | None = None) -> str:
    """Render an MDX template using Python format variables.

    Pipelines can reference variables like `{myview_id}`.

    MDX itself contains lots of `{ ... }` sets. To avoid forcing heavy escaping
    in YAML, we only substitute variables in the form `${var}`.
    """
    if not variables:
        return mdx

    rendered = mdx
    for key, value in variables.items():
        rendered = rendered.replace(f"${{{key}}}", str(value))
    return rendered


def _repo_root() -> str:
    return os.path.dirname(os.path.abspath(os.path.dirname(__file__)))


def load_pipelines(pipelines_file: str | None = None) -> Dict[str, PipelineDefinition]:
    root = _repo_root()
    path = pipelines_file or os.path.join(root, "pipelines", "pipelines.yaml")

    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    pipelines = data.get("pipelines") or {}
    out: Dict[str, PipelineDefinition] = {}

    for name, cfg in pipelines.items():
        if not isinstance(cfg, dict):
            raise ValueError(f"Pipeline '{name}' must be a mapping")
        mdx = (cfg.get("mdx") or "").strip("\n")
        parser = cfg.get("parser")
        mapping = cfg.get("mapping")
        catalog = cfg.get("catalog")
        hierarchy_mappings = cfg.get("hierarchy_mappings")
        if not mdx:
            raise ValueError(f"Pipeline '{name}' is missing mdx")
        if not parser:
            raise ValueError(f"Pipeline '{name}' is missing parser")
        if not mapping:
            raise ValueError(f"Pipeline '{name}' is missing mapping")

        mapping_path = mapping
        if not os.path.isabs(mapping_path):
            mapping_path = os.path.join(root, "pipelines", mapping_path)

        out[name] = PipelineDefinition(
            name=name,
            mdx=mdx,
            parser=str(parser),
            mapping_path=mapping_path,
            catalog=str(catalog) if catalog else None,
            hierarchy_mappings=hierarchy_mappings,
        )

    return out


def load_mapping(mapping_path: str) -> Dict[str, Any]:
    with open(mapping_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Mapping file must be a YAML object: {mapping_path}")
    return data
