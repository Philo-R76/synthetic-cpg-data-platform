import unittest

from src.generate_cpg_data import PRODUCTS
from src.ontology import (
    DEFAULT_ONTOLOGY_PATH,
    build_alias_index,
    load_ontology,
    resolve_product,
)


class OntologyTests(unittest.TestCase):
    def setUp(self):
        self.ontology = load_ontology(DEFAULT_ONTOLOGY_PATH)
        self.aliases, self.fuzzy_aliases = build_alias_index(
            list(PRODUCTS), self.ontology, "v1.3"
        )

    def test_curated_synonym_maps_with_high_confidence(self):
        product_id, method, confidence = resolve_product(
            "Northstar Original Oat Beverage",
            self.aliases,
            self.fuzzy_aliases,
            self.ontology,
            allow_fuzzy=True,
        )
        self.assertEqual(product_id, "SKU-1001")
        self.assertEqual(method, "ontology_synonym")
        self.assertEqual(confidence, 0.97)

    def test_fuzzy_lexical_match_maps_a_near_alias_but_not_ambiguous_values(self):
        product_id, method, confidence = resolve_product(
            "Northstar Oat Drink Origina",
            self.aliases,
            self.fuzzy_aliases,
            self.ontology,
            allow_fuzzy=True,
        )
        self.assertEqual(product_id, "SKU-1001")
        self.assertEqual(method, "fuzzy_lexical")
        self.assertGreaterEqual(confidence, 0.88)

        product_id, method, confidence = resolve_product(
            "unknown beverage product",
            self.aliases,
            self.fuzzy_aliases,
            self.ontology,
            allow_fuzzy=True,
        )
        self.assertIsNone(product_id)
        self.assertEqual(method, "low_conf_hitl")
        self.assertEqual(confidence, 0)

    def test_earlier_release_uses_only_its_declared_alias_features(self):
        aliases, fuzzy_aliases = build_alias_index(
            list(PRODUCTS), self.ontology, "v1.0"
        )
        product_id, method, _ = resolve_product(
            "Northstar Original Oat Beverage",
            aliases,
            fuzzy_aliases,
            self.ontology,
            allow_fuzzy=False,
        )
        self.assertIsNone(product_id)
        self.assertEqual(method, "low_conf_hitl")


if __name__ == "__main__":
    unittest.main()
