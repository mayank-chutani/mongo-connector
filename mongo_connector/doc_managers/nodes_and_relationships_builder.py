"""
Class that builds Nodes and Relationships
according to a Mongo document.
"""
import re
import logging
from mongo_connector.constants import GEO_KEY

LOG = logging.getLogger(__name__)


class NodesAndRelationshipsBuilder(object):
    def __init__(self, doc, doc_type, doc_id, metadata={}, doc_types=[]):
        self.doc_id = doc_id
        self.query_nodes = {}
        self.relationships_query = {}
        self.metadata = metadata or {}
        self.doc_types = doc_types or []
        self.explicit_ids = {}
        self.cypher_list = []
        self.statements_with_params = []
        # normalized_doc = self._normalize_geo_key(doc_type, doc, doc_id)
        self.build_nodes_query(doc_type, doc, doc_id)

    # def _normalize_geo_key(self, doc_type, document, id):


    def build_nodes_query(self, doc_type, document, id):
        self.doc_types.append(doc_type)
        parameters = {'uid': id}
        # parameters = {}
        if self.is_dict(self.metadata):
            parameters.update(self.metadata)
        for key in document.keys():
            if self.is_reference(key):
                self.build_node_with_reference(doc_type, key, id, document[key])
                continue
            if document[key] is None:
                continue
            elif key == GEO_KEY and self.is_list(document[key]) and len(document[key]) == 2:
                geo = document[key]
                lat = geo[0]
                lon = geo[1]
                if isinstance(lat, float) and isinstance(lon, float):
                    document.pop(key)
                    parameters.update({'lat': lat, 'lon': lon})
            elif self.is_dict(document[key]):
                # Expecting _id in nested json
                if 'uid' not in document[key].keys():
                    LOG.error('uid not present.')
                    continue
                nested_id = document[key]['uid']
                self.build_relationships_query(doc_type, key, id, nested_id)
                self.build_nodes_query(key, document[key], nested_id)
            elif self.is_json_array(document[key]):
                for json in self.format_params(document[key]):
                    json_key = key + str(document[key].index(json))
                    self.build_relationships_query(doc_type, json_key, id, id)
                    self.build_nodes_query(json_key, json, id)
            elif self.is_multimensional_array(document[key]):
                parameters.update(self.flatenned_property(key, document[key]))
            else:
                parameters.update({key: self.format_params(document[key])})

        if isinstance(id, str) or isinstance(id, unicode):
            node_query = 'MERGE (v:`{doc_type}` {{uid: "{id}"}}) return id(v)'.format(id=id, doc_type=doc_type)
            match_query = 'MATCH (n:`{doc_type}` {{uid: "{id}"}})'.format(id=id, doc_type=doc_type)
        else:
            node_query = 'MERGE (v:`{doc_type}` {{uid: {id}}}) return id(v)'.format(id=id, doc_type=doc_type)
            match_query = 'MATCH (n:`{doc_type}` {{uid: "{id}"}})'.format(id=id, doc_type=doc_type)

        self.cypher_list.append(node_query)

        # TODO: FIx duplicate node creation with this code
        new_params = {'parameters': parameters}
        params_query = '{match_query} SET n+={{parameters}}'.format(match_query=match_query)
        self.statements_with_params.append({params_query: new_params})

        # Working code
        # for key, val in parameters.iteritems():
        #     if isinstance(val, str) or isinstance(val, unicode):
        #         params_query = '{match_query} set n.{key}="{value}"'.format(key=key, value=val, match_query=match_query)
        #     else:
        #         params_query = '{match_query} set n.{key}={value}'.format(key=key, value=val, match_query=match_query)
        #     self.cypher_list.append(params_query)

    def format_params(self, params):
        if (type(params) is list):
            return list(filter(None, params))
        return params

    def build_node_with_reference(self, root_type, key, doc_id, document_key):
        if document_key is None:
            return
        doc_type = key.split("uid")[0]

        # ignore uid property of subdocuments
        if not doc_type or doc_type == "":
            return

        parameters = {'uid': document_key}
        statement = "MERGE (d:`{doc_type}` {{ uid: {{parameters}}.uid}})".format(doc_type=doc_type)
        self.query_nodes.update({statement: {"parameters": parameters}})
        self.build_relationships_query(root_type, doc_type, doc_id, document_key)
        self.explicit_ids.update({document_key: doc_type})

    def is_dict(self, doc_key):
        return (type(doc_key) is dict)

    def is_list(self, doc_key):
        return (type(doc_key) is list)

    def is_reference(self, key):
        return (re.search(r"uid$", key))

    def is_multimensional_array(self, doc_key):
        return ((type(doc_key) is list) and (doc_key) and (type(doc_key[0]) is list))

    def flatenned_property(self, key, doc_key):
        parameters = {}
        flattened_list = doc_key
        if ((type(flattened_list[0]) is list) and (flattened_list[0])):
            inner_list = flattened_list[0]
            if (type(inner_list[0]) is list):
                flattened_list = [val for sublist in flattened_list for val in sublist]
                self.flatenned_property(key, flattened_list)
            else:
                for element in flattened_list:
                    element_key = key + str(flattened_list.index(element))
                    parameters.update({element_key: element})
        return parameters

    def is_json_array(self, doc_key):
        return ((type(doc_key) is list) and (doc_key) and (type(doc_key[0]) is dict))

    def build_relationships_query(self, main_type, node_type, doc_id, explicit_id):
        relationship_type = main_type + "_" + node_type
        statement = "MATCH (a:`{main_type}`), (b:`{node_type}`) WHERE a.uid={{doc_id}} AND b.uid ={{explicit_id}} CREATE UNIQUE (a)-[r:`{relationship_type}`]->(b)".format(
            main_type=main_type, node_type=node_type, relationship_type=relationship_type)
        params = {"doc_id": doc_id, "explicit_id": explicit_id}
        self.relationships_query.update({statement: params})
