"""Saved upstream declarations, inverted downstream and incomplete coverage."""
import json
import unittest

from lineage_app.data.dag_tags import dag_tag_graph,inherited_projects
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


    def test_projects_pass_to_every_upstream_level_only(self):
        # A -> B -> C and A -> 2 -> 3; only C has a project.
        dags=[{'dag_id':'A','tags':[]},{'dag_id':'B','tags':['upstream:A']},{'dag_id':'C','tags':['upstream:B','project:pay']},
              {'dag_id':'2','tags':['upstream:A']},{'dag_id':'3','tags':['upstream:2']}]
        got=inherited_projects(dags)
        self.assertEqual(got,{'A':{'pay':['C']},'B':{'pay':['C']}})  # 2 and 3 inherit nothing.

    def test_a_middle_project_goes_up_not_down_and_projects_combine(self):
        dags=[{'dag_id':'A','tags':[]},{'dag_id':'B','tags':['upstream:A',' project:loan']},{'dag_id':'C','tags':['upstream:B','project:pay']},
              {'dag_id':'D','tags':['upstream:A','project:card']}]
        got=inherited_projects(dags)
        self.assertNotIn('C',got)  # B's project never goes downstream to C.
        self.assertEqual(got['B'],{'pay':['C']})
        self.assertEqual(got['A'],{'card':['D'],'loan':['B'],'pay':['C']})

    def test_unknown_and_own_projects_are_not_inherited_and_links_cross_airflows(self):
        dags=[{'dag_id':'dp_a','tags':['project:pay'],'airflow_source':'Data Platform'},
              {'dag_id':'risk_b','tags':['project:unknown','project:','project:none'],'airflow_source':'Risk Team'},
              {'dag_id':'dp_c','tags':['upstream:dp_a','project:pay']}]
        # Airflow's own link (Risk -> DP) counts like a tag.
        links=[{'upstream_dag_id':'dp_a','downstream_dag_id':'risk_b','link_kind':'sensor','evidence':'rendered_fields','resolution':'cross_airflow'}]
        self.assertEqual(inherited_projects(dags,links),{})  # unknown/empty/none never pass; dp_a already owns pay.
        dags[1]['tags']=['project:risk_mart']
        self.assertEqual(inherited_projects(dags,links),{'dp_a':{'risk_mart':['risk_b']}})

    def test_cycles_do_not_loop_or_inherit_a_dags_own_project(self):
        got=inherited_projects([{'dag_id':'A','tags':['upstream:B','project:x']},{'dag_id':'B','tags':['upstream:A']}])
        self.assertEqual(got,{'B':{'x':['A']}})


if __name__=='__main__':unittest.main()
