"""
Neo4j implementation for the DocManager. Receives documents and 
communicates with Neo4j Server.
"""
import base64
import logging
import os
import os.path as path, sys

import bson.json_util

from mongo_connector.doc_managers.nodes_and_relationships_builder import NodesAndRelationshipsBuilder
from mongo_connector.doc_managers.nodes_and_relationships_updater import NodesAndRelationshipsUpdater
from mongo_connector.doc_managers.error_handler import ErrorHandler

from py2neo import Graph, authenticate


from mongo_connector import errors
from mongo_connector.compat import u
from mongo_connector.constants import (DEFAULT_COMMIT_INTERVAL,
                                       DEFAULT_MAX_BULK)
from mongo_connector.util import exception_wrapper, retry_until_ok
from mongo_connector.doc_managers.doc_manager_base import DocManagerBase
from mongo_connector.doc_managers.formatters import DefaultDocumentFormatter

import requests
import urlparse
import json
import os

req = requests.Session()

errors_handler = ErrorHandler()
wrap_exceptions = exception_wrapper(errors_handler.error_hash)

LOG = logging.getLogger(__name__)

class DocManager(DocManagerBase):
    """
    Neo4j implementation for the DocManager. Receives documents and
    communicates with Neo4j Server.
    """

    def __init__(self, url, auto_commit_interval=DEFAULT_COMMIT_INTERVAL,
                 unique_key='uid', chunk_size=DEFAULT_MAX_BULK, **kwargs):

        self.graph = Graph(url)
        self.url = url
        self.auto_commit_interval = auto_commit_interval
        self.unique_key = unique_key
        self.chunk_size = chunk_size
        self._formatter = DefaultDocumentFormatter()
        self.kwargs = kwargs.get("clientOptions")
        self.authorization_token = base64.b64encode(os.getenv('NEO4J_AUTH'))

    def apply_id_constraint(self, doc_types):
        for doc_type in doc_types:
            constraint = "CREATE CONSTRAINT ON (d:`{doc_type}`) ASSERT d.uid IS UNIQUE".format(doc_type=doc_type)
            self.graph.cypher.execute(constraint)

    def stop(self):
        """Stop the auto-commit thread."""
        self.auto_commit_interval = None

    @wrap_exceptions
    def upsert(self, doc, namespace, timestamp):
        """Inserts a document into Neo4j."""
        index, doc_type = self._index_and_mapping(namespace)
        doc_id = u(doc.pop("uid"))
        metadata = { "_ts": timestamp }
        doc = self._formatter.format_document(doc)
        builder = NodesAndRelationshipsBuilder(doc, doc_type, doc_id, metadata)
        self.apply_id_constraint(builder.doc_types)
        tx = self.graph.cypher.begin()
        for statement in builder.query_nodes.keys():
            tx.append(statement, builder.query_nodes[statement])
        for query in builder.cypher_list:
            tx.append(query)
            # Adding cyphers from cypher list
        for relationship in builder.relationships_query.keys():
            tx.append(relationship, builder.relationships_query[relationship])
        for statement in builder.statements_with_params:
            for key in statement.keys():
                tx.append(key, statement[key])
        commit_result = None
        try:
            commit_result = tx.commit()
            print commit_result
        except Exception as e:
            LOG.error('{}'.format(e.message))
            pass

        if commit_result:
            nodeids_list = self._get_nodeids(commit_result)
            self.create_geospatial_indices(nodeids_list)

    def _get_nodeids(self, commit_result):
        node_id_list = []
        a = len(commit_result)
        for i in range(len(commit_result)):
            res = commit_result.pop(0)
            records = res.records
            if not records:
                continue
            for record in records:
                node_ids = list(record.__values__)
                node_id_list.extend(node_ids)
        return node_id_list

    def create_geospatial_indices(self, node_ids_list):
        """
        Creates geo spatial indices on the node ids
        :param node_ids_list:  list of node ids
        """
        layer_name = 'geom'
        lat = 'lat'
        lon = 'lon'
        geometry_type = 'point'
        self._set_id_to_nodeid(node_ids_list)
        # if_layer = self.if_layer_exists(layer_name)
        # if if_layer:
        self._create_layer(layer_name, lat, lon)
        self._add_geometry(layer_name, geometry_type, lat, lon)
        result = self._add_node_to_layer(node_ids_list, layer_name)
        LOG.info('Geospatial index creation response {}', repr(result))

    def _set_id_to_nodeid(self, node_ids_list):
        # TODO: We may want it to change to label name
        """
        Set id on basis of node ids
        :param node_ids_list:
        :param label_name:
        :return:
        """
        tx = self.graph.cypher.begin()
        for count, nodeid in enumerate(node_ids_list, 1):
            if count % 1000 == 0:
                tx.commit()
                tx = self.graph.cypher.begin()
            query = 'MATCH (n) where id(n) = {nodeid} set n.id={nodeid}'.format(nodeid=nodeid)
            tx.append(query)
        if not tx.finished:
            tx.commit()

    def _add_node_to_layer(self, node_ids_list, layer_name):
        """
        Adds nodes to layer
        :param node_ids_list: list of node ids
        :param layer_name: <string>
        :return: [(nodeid, res)]
        """
        endpoint = '/ext/SpatialPlugin/graphdb/addNodeToLayer'
        url = self.url + endpoint
        result_list = []
        for nodeid in node_ids_list:
            node_endpoint = '/node/{}'.format(nodeid)
            node = self.url + node_endpoint
            payload = {'layer': layer_name,
                       'node': node}
            res = self._post_request(url, payload=payload)
            result_list.append((nodeid, res))
        return result_list

    def _add_geometry(self, layer_name, geometry_type, lat, lon):
        """ Creates a geometry """
        endpoint = '/index/node'
        url = self.url + endpoint
        payload = {'name': layer_name,
                              'config': {
                                  'provider': 'spatial',
                                  'geometry_type': geometry_type,
                                  'lat': lat,
                                  'lon': lon}
                              }
        res = self._post_request(url, payload=payload)
        if res.status_code == 201:
            LOG.info('Geometry {} created'.format(geometry_type))
            return True, res
        else:
            LOG.error('Gometry creation error: {}'.format(geometry_type))
            return False, res

    def _create_layer(self, layer_name, lat, lon):
        """
        Creates a layer
        :param layer_name: <string>
        :return: (<bool>, res)
        """
        endpoint = '/ext/SpatialPlugin/graphdb/addSimplePointLayer'
        url = self.url + endpoint
        payload = {'layer': layer_name,
                              'lat': lat,
                              'lon': lon}
        res = self._post_request(url, payload=payload)
        if res.status_code == 200:
            LOG.info('Layer \'{}\' created successfully'.format(layer_name))
            return True, res
        else:
            LOG.error('Layer creation error code: {} - {}'.format(res.status_code, res))
            return False, res

    def _post_request(self, url, payload):
        payload = json.dumps(payload)
        headers = {'authorization': self.authorization_token,
                   'content-type': 'application/json'}
        res = req.post(url, data=payload, headers=headers)
        return res

    def if_layer_exists(self, layer_name):
        endpoint = '/ext/SpatialPlugin/graphdb/getLayer'
        url = self.url + endpoint
        payload = {'layer': layer_name}
        res = self._post_request(url, payload=payload)
        if res.status_code == 200:
            return True
        else:
            return False

    @wrap_exceptions
    def bulk_upsert(self, docs, namespace, timestamp):
        """Insert multiple documents into Neo4j."""
        """Maximum chunk size is 1000. Transaction blocks won't have more than 1000 statements."""
        metadata = { "_ts": timestamp }
        tx = self.graph.cypher.begin()
        for doc in docs:
            index, doc_type = self._index_and_mapping(namespace)
            doc_id = u(doc.pop("uid"))
            doc = self._formatter.format_document(doc)
            builder = NodesAndRelationshipsBuilder(doc, doc_type, doc_id, metadata)
            self.apply_id_constraint(builder.doc_types)
            for statement in builder.query_nodes.keys():
                tx.append(statement, builder.query_nodes[statement])
            for relationship in builder.relationships_query.keys():
                tx.append(relationship, builder.relationships_query[relationship])
        try:
            tx.commit()
        except Exception as e:
            LOG.error('{}'.format(e.message))
            pass

    @wrap_exceptions
    def update(self, document_id, update_spec, namespace, timestamp):
        doc_id = u(document_id)
        tx = self.graph.cypher.begin()
        index, doc_type = self._index_and_mapping(namespace)
        updater = NodesAndRelationshipsUpdater()
        updater.run_update(update_spec, doc_id, doc_type)
        for statement in updater.statements_with_params:
            for key in statement.keys():
                tx.append(key, statement[key])
        tx.commit()

    @wrap_exceptions
    def remove(self, document_id, namespace, timestamp):
        """Removes a document from Neo4j."""
        doc_id = u(document_id)
        index, doc_type = self._index_and_mapping(namespace)
        params_dict = {"doc_id": doc_id}
        tx = self.graph.cypher.begin()
        statement = "MATCH (d:Document) WHERE d.uid={doc_id} OPTIONAL MATCH (d)-[r]-() DELETE d, r"
        tx.append(statement, params_dict)
        tx.commit()

    @wrap_exceptions
    def search(self, start_ts, end_ts):
        statement = "MATCH (d:Document) WHERE d._ts>={start_ts} AND d._ts<={end_ts} RETURN d".format(start_ts=start_ts, end_ts=end_ts)
        results = self.graph.cypher.execute(statement)
        return results


    def commit(self):
        LOG.error("Commit")


    @wrap_exceptions
    def get_last_doc(self):
        """Get the most recently modified node from Neo4j.
        This method is used to help define a time window within which documents
        may be in conflict after a MongoDB rollback.
        """
        LOG.error("Commit")


    def handle_command(self, doc, namespace, timestamp):
        db = namespace.split('.', 1)[0]

    def _index_and_mapping(self, namespace):
        """Helper method for getting the index and type from a namespace."""
        index, doc_type = namespace.split('.', 1)
        return index.lower(), doc_type
