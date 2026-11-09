"""Saved upstream declarations, inverted downstream and incomplete coverage."""
import json
import unittest

from lineage_app.data.dag_tags import dag_tag_graph
from lineage_app.data.model import trace


class DAGTagTests(unittest.TestCase):
    def test_repeated_and_comma_separated_tags_are_deduplicated_and_reversed(self):
        graph=dag_tag_graph([
            {'dag_id':'A','tags':['upstream:none']},
            {'dag_id':'B','tags':[{'name':'upstream:A'},' upstream : A, C ','upstream:C']},
        ])
        self.assertEqual(graph['edges'],[('A','B'),('C','B')])
        self.assertEqual(graph['up']['B'],{'A','C'})
        self.assertEqual(graph['down']['A'],{'B'})
        self.assertEqual(graph['coverage'],{'A':'complete','B':'complete'})

    def test_unknown_empty_and_invalid_values_never_become_nodes(self):
        graph=dag_tag_graph([
            {'dag_id':'A','tags':['upstream:unknown']},
            {'dag_id':'B','tags':['upstream:A, unknown','upstream:','upstream:B','upstream:<script>']},
            {'dag_id':'C','tags':['project:upstream:A']},
        ])
        self.assertEqual(graph['edges'],[('A','B')])
        self.assertEqual(graph['coverage'],{'A':'unknown','B':'partial','C':'missing'})
        self.assertEqual(graph['down']['A'],{'B'})

    def test_no_tags_does_not_invent_links_and_explicit_none_is_distinct(self):
        graph=dag_tag_graph([{'dag_id':'A','tags':[]},{'dag_id':'B','tags':['upstream:none']},
                             {'dag_id':'C','tags':['upstream:none','upstream:A']}])
        self.assertEqual(graph['coverage'],{'A':'missing','B':'complete','C':'partial'})
        self.assertEqual(graph['edges'],[('A','C')])

    def test_raw_inventory_json_uses_the_same_parser_and_rejects_malformed_data(self):
        graph=dag_tag_graph([
            {'dag_id':'B','raw_json':json.dumps({'tags':[{'name':'upstream:A'}]})},
            {'dag_id':'C','raw_json':'bad json'},
            {'dag_id':'D','raw_json':'[]'},
            {'dag_id':'E','tags':'upstream:A'},
        ])
        self.assertEqual(graph['edges'],[('A','B')])
        self.assertTrue(all(graph['coverage'][d]=='missing' for d in ('C','D','E')))

    def test_cycles_are_safe_for_direct_and_all_level_traces(self):
        graph=dag_tag_graph([{'dag_id':'A','tags':['upstream:C']},
                             {'dag_id':'B','tags':['upstream:A']},
                             {'dag_id':'C','tags':['upstream:B']}])
        self.assertEqual(trace('A',graph['down'],False),{'B'})
        self.assertEqual(trace('A',graph['down'],True),{'B','C'})


if __name__=='__main__':unittest.main()
