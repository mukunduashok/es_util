"""
Utility module to hold all the helpers required to perform actions in elasticsearch
"""

# Global imports

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import os

class ElasticSearchUtils(object):

    def __init__(self, es_user=None, es_password=None, es_host=None, es_port=None, ca_path=None, verify_certs=False, protocol="http"):
        """
        Provide params while creating ES instance if using ES instance other than default
        :param es_user: username for custom ES
        :param es_password: password for custom ES
        :param es_host: host for custom ES
        :param es_port: port for custom ES
        """
        # Construct credentials for ES connection
        self.es_username = es_user
        self.es_password = es_password
        self.es_host = es_host
        self.es_port = es_port
        self.ca_path = ca_path
        self.verify_certs = verify_certs
        self.protocol = protocol

        self.es = self.connect_to_es()

    def connect_to_es(self):
        """
        Utility to connect to elastic search
        :return: es connection object
        """
        return Elasticsearch(
            hosts=[{'host': self.es_host, "port": self.es_port}],
            scheme=self.protocol,
            http_auth=(self.es_username, self.es_password),
            ca_certs=self.ca_path,
            verify_certs=self.verify_certs
        )

    def index_data(self, index_name, doc_type, body):
        """
        Indexes the data to ES
        :param index_name: index name of the doc to be indexed
        :param doc_type: type of doc to be indexed
        :param body: document to index into elastic search
        :return: True -> doc indexed successfully. False -> problem with elastic search indexing.
        """

        self.es.index(index=index_name, body=body, doc_type=doc_type)
        return True

    def update_data(self, index_name, id_doc, doc_type, body):
        """
        Indexes the data to ES
        :param index_name: index name of the doc to be updated
        :param id_doc: id needs to be updated
        :param doc_type: type of doc to be updated
        :param body: document to update into elastic search
        :return: True -> doc updated successfully. False -> problem with elastic search updating.
        """

        self.es.update(index=index_name, id=id_doc, doc_type=doc_type, body=body)
        self.refresh_data(index_name)
        return True

    def delete_data(self, index_name, id_doc, doc_type):
        """
        Indexes the data to ES
        :param index_name: index name of the doc to be deleted
        :param id_doc: id needs to be deleted
        :param doc_type: type of doc to be indexed
        :return: True -> doc deleted successfully. False -> problem with elastic search deleting.
        """

        self.es.delete(index=index_name, id=id_doc, doc_type=doc_type)
        self.refresh_data(index_name)
        return True

    def bulk_index_data(self, index_name, doc_type, bulk_body):
        """
        indexes huge data at once. Can be used when number of records to index is huge (Say > 1000)
        :param index_name: index name of the doc to be indexed
        :param doc_type: type of doc to be indexed
        :param bulk_body: list of document body dict containing "_source" records
        :return: number of success records if all fine.
        errors if failure during bulk index
        None if exception during bulk index
        """

        def yield_bulk_data(data_list):
            for item in data_list:
                yield {
                    "_index": index_name,
                    "_type": doc_type,
                    "_source": item
                }

        return bulk(self.es, yield_bulk_data(bulk_body), request_timeout=45)

    def search_data(self, index_name, filter_params=None, custom_filter=None):
        """
        Search for records based on the filter provided
        :param index_name: index name for which the search is to be executed
        :param filter_params: list of dict containing key: value filters
        :param custom_filter: https://www.elastic.co/guide/en/elasticsearch/reference/current/query-filter-context.html
        :return: filtered records from es
        Note: Either of filter_params or custom_filter is mandatory
        """
        self.refresh_data(index_name)
        if filter_params:
            filter_query = []
            [filter_query.append({"match": filter_string}) for filter_string in filter_params]
            search_result = self.es.search(index=index_name, body={"query": {"bool": {"must": filter_query}}, "size": 5000}, request_timeout=45)
        else:
            search_result = self.es.search(
                index=index_name, body=custom_filter, request_timeout=45)
        return search_result["hits"]["hits"]

    def get_aggregated_data(self, index_name, custom_filter):
        """
        Function to use if pulling data from an aggregation query
        :param index_name: index name for which the search is to be executed
        :param custom_filter: https://www.elastic.co/guide/en/elasticsearch/reference/current/query-filter-context.html
        :return: aggregated records from es
        """
        self.refresh_data(index_name)
        es_data = self.es.search(index=index_name, body=custom_filter)
        return es_data['aggregations']

    def get_data(self, index_name, custom_filter):
        """
        Search for records based on the filter provided
        :param index_name: index name for which the search is to be executed
        :param custom_filter: https://www.elastic.co/guide/en/elasticsearch/reference/current/query-filter-context.html
        :return: filtered records from es
        """
        self.refresh_data(index_name)
        full_data = []
        es_data = self.es.search(
            index=index_name, body=custom_filter, size=10000, scroll='2m', request_timeout=45)
        # Get the scroll ID
        sid = es_data['_scroll_id']
        # Get the number of results returned
        scroll_size = len(es_data['hits']['hits'])
        # Append existing data to full data
        full_data = full_data + (es_data['hits']['hits'])

        while scroll_size > 0:
            es_data = self.es.scroll(scroll_id=sid, scroll='2m')
            # Update the scroll ID
            sid = es_data['_scroll_id']
            # Get the number of results that returned in the last scroll
            scroll_size = len(es_data['hits']['hits'])
            full_data = full_data + (es_data['hits']['hits'])

        return full_data

    def get_date(self, index_name, filter_params=None, latest=True, sort_key="data_date", get_id=False):
        """
        Get the first or last available date for the specified index or / and filter params
        :param index_name: index name for which the search is to be executed
        :param filter_params: list of dict containing key: value filters
        :param latest: if True, returns date for latest available data else returns date for oldest available data
        :param sort_key: The key mapping for the date string / object
        :param get_id: If the _id is needed this should be st true
        :return: Date string / object if success. None if failed.
        """
        self.refresh_data(index_name)
        if filter_params is None:
            data = self.es.search(index=index_name,
                                  body={"query": {"match_all": {}}, "sort": {sort_key: "DESC" if latest else "ASC"},
                                        "_source": {"includes": [sort_key]}},
                                  request_timeout=45)
        else:
            data = self.search_data(index_name, filter_params)
        if get_id:
            return data["hits"]["hits"][0]["_id"]
        return data["hits"]["hits"][0]["_source"][sort_key]

    def refresh_data(self, index_name):
        """
        Refreshes the data for the specified index, so as to appear up to date data during search
        :param index_name: Name of the index
        :return: None
        """
        self.es.indices.refresh(index=index_name)


if __name__ == '__main__':
    pass
