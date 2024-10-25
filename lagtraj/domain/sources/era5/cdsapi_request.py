import cdsapi
from cdsapi.api import Result
import requests

class RequestFetchCDSClient():
    class RequestNotFoundException(Exception):
        pass

    """
    Wraps CDS api so that we can submit a request, get the request id and then
    later query the status or download data based on a request ID.
    """

    def __init__(self, *args, **kwargs):
        # LD: looking at the code forget=True avoids the cdsapi client sleeping
        # and waiting for the request to complete
        self.client=cdsapi.Client()

    def queue_data_request(self, repository_name, query_kwargs):
        response = self.client.retrieve(repository_name, query_kwargs)
        request_id = response.url.split('/')[-2]
        request_url = response.location
        return request_id, request_url

    def download_data_by_request(self, request_url, target):
        res = requests.get(request_url, stream = True)
        print("Writing data to " + target)
        with open(target,'wb') as fh:
            for r in res.iter_content(chunk_size = 1024):
                fh.write(r)
        fh.close()

    def _get_request_status(self, request_url):
        session = self.client.session
        result = self.client.robust(session.get)(
            request_url, verify=self.client.verify, timeout=self.client.timeout
        )
        return result

    def get_request_status(self, request_url):
        reply = self._get_request_status(request_url=request_url)
        return reply
