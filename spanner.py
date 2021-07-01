from google.cloud import storage


class Spanner:
    def __init__(self):
        pass

    def ingest(self, source: dict = {}):
        """Ingest spans from a given source

        Args:
            source (dict): source connection details

        """
        if "gcp" in source:
            bucket = source["gcp"]["bucket"]

            storage_client = storage.Client()
            blobs = storage_client.list_blobs(bucket)
