"""Ontology loading and deterministic product-identity resolution."""

from __future__ import annotations

import json
import re
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any


DEFAULT_ONTOLOGY_PATH = Path(__file__).resolve().parents[1] / "config" / "product_ontology.json"
BASE_FEATURES = {
    "canonical_sku",
    "product_name",
    "brand_product",
    "branded_pack",
    "ontology_synonyms",
    "fuzzy_lexical",
}


def load_ontology(path: Path = DEFAULT_ONTOLOGY_PATH) -> dict[str, Any]:
    if not path.is_file():
        raise FileNotFoundError(f"Product ontology not found: {path}")
    ontology = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(ontology.get("entities"), dict):
        raise ValueError("Ontology must define an entities object")
    releases = ontology.get("mapping_releases")
    if not isinstance(releases, list) or not releases:
        raise ValueError("Ontology must define at least one mapping release")
    versions = [release.get("version") for release in releases]
    if len(versions) != len(set(versions)) or any(not version for version in versions):
        raise ValueError("Ontology mapping release versions must be unique and non-empty")
    for release in releases:
        features = release.get("features")
        if not isinstance(features, list) or not set(features).issubset(BASE_FEATURES):
            raise ValueError(f"Invalid feature list for mapping release {release.get('version')!r}")
    fuzzy = ontology.get("fuzzy_lexical", {})
    for key in ("minimum_confidence", "minimum_margin"):
        value = fuzzy.get(key)
        if not isinstance(value, (int, float)) or not 0 <= value <= 1:
            raise ValueError(f"Ontology fuzzy_lexical.{key} must be between 0 and 1")
    return ontology


def normalize_product_value(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.casefold())


def build_alias_index(
    products: list[dict[str, Any]],
    ontology: dict[str, Any],
    version: str,
) -> tuple[dict[str, tuple[str, str]], dict[str, str]]:
    release = next(
        (item for item in ontology["mapping_releases"] if item["version"] == version),
        None,
    )
    if release is None:
        raise ValueError(f"Unknown ontology mapping release: {version}")
    features = set(release["features"])
    aliases: dict[str, tuple[str, str]] = {}
    fuzzy_aliases: dict[str, str] = {}
    ontology_ids = set(ontology["entities"])
    product_ids = {product["product_id"] for product in products}
    unknown_ids = ontology_ids - product_ids
    if unknown_ids:
        raise ValueError(f"Ontology contains unknown product IDs: {sorted(unknown_ids)}")

    for product in products:
        product_id = product["product_id"]
        candidate_aliases: list[tuple[str, str]] = []
        if "canonical_sku" in features:
            candidate_aliases.append((product["canonical_sku"], "rule_exact"))
        if "product_name" in features:
            candidate_aliases.append((product["product_name"], "rule_exact"))
        if "brand_product" in features:
            candidate_aliases.append(
                (f'{product["brand"]} {product["product_name"]}', "rule_exact")
            )
        if "branded_pack" in features:
            candidate_aliases.append(
                (
                    f'{product["brand"]} {product["product_name"]} '
                    f'{product["pack_size"]}{product["pack_unit"]}',
                    "rule_exact",
                )
            )
        if "ontology_synonyms" in features:
            synonyms = ontology["entities"][product_id].get("synonyms", [])
            if not isinstance(synonyms, list) or any(
                not isinstance(synonym, str) or not synonym.strip()
                for synonym in synonyms
            ):
                raise ValueError(f"Invalid ontology synonyms for product {product_id}")
            candidate_aliases.extend(
                (synonym, "ontology_synonym") for synonym in synonyms
            )

        for alias, method in candidate_aliases:
            normalized = normalize_product_value(alias)
            existing = aliases.get(normalized)
            if existing is not None and existing[0] != product_id:
                raise ValueError(f"Product alias collision: {alias!r}")
            if existing is None or method == "rule_exact":
                aliases[normalized] = (product_id, method)
            fuzzy_aliases[normalized] = product_id

    return aliases, fuzzy_aliases


def resolve_product(
    raw_value: str | None,
    aliases: dict[str, tuple[str, str]],
    fuzzy_aliases: dict[str, str],
    ontology: dict[str, Any],
    allow_fuzzy: bool,
) -> tuple[str | None, str, float]:
    if not raw_value or not raw_value.strip():
        return None, "low_conf_hitl", 0.0
    normalized = normalize_product_value(raw_value)
    exact = aliases.get(normalized)
    if exact is not None:
        product_id, method = exact
        confidence = 0.97 if method == "ontology_synonym" else 1.0
        return product_id, method, confidence
    if allow_fuzzy:
        best_by_product: dict[str, float] = {}
        for alias, product_id in fuzzy_aliases.items():
            score = SequenceMatcher(None, normalized, alias).ratio()
            best_by_product[product_id] = max(score, best_by_product.get(product_id, 0.0))
        ranked = sorted(best_by_product.items(), key=lambda item: item[1], reverse=True)
        if ranked:
            best_product, best_score = ranked[0]
            next_score = ranked[1][1] if len(ranked) > 1 else 0.0
            fuzzy_config = ontology["fuzzy_lexical"]
            if (
                best_score >= fuzzy_config["minimum_confidence"]
                and best_score - next_score >= fuzzy_config["minimum_margin"]
            ):
                return best_product, "fuzzy_lexical", best_score
    return None, "low_conf_hitl", 0.0
