import cdsapi
from cdsapi.api import Result
import os


class RequestFetchCDSClient:
    class RequestNotFoundException(Exception):
        pass

    """
    Wraps CDS api so that we can submit a request, get the request id and then
    later query the status or download data based on a request ID.
    """

    def __init__(self, *args, **kwargs):
        cdsapi_client = cdsapi.Client()
        self.client = cdsapi_client.client

    def queue_data_request(self, repository_name, query_kwargs):
        collection = self.client.get_collection(repository_name)
        collection.process.apply_constraints(**query_kwargs)
        remote = self.client.submit(repository_name, **query_kwargs)
        print("submitted request, status")
        print(remote.status)
        return remote.request_uid

    def download_data_by_request(self, request_id, target):
        remote = self.client.get_remote(request_id)
        remote.download(target=target)

    def _get_request_status(self, request_id):
        remote = self.client.get_remote(request_id)
        return remote.status

    def get_request_status(self, request_id):
        reply = self._get_request_status(request_id=request_id)
        return reply
