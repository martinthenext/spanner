from google.cloud import storage
from functools import reduce
from typing import Optional


def pipeline(*steps):
    return reduce(lambda x, y: y(x), list(steps))


class Span:
    # id 
    layer: str
    app: str
    author: str
    context: str
    start: int

    # data
    content: str


class Spanner:
    def __init__(self):
        pass

    def ingest(
        self, layer: str, source: dict = {}, app=None, context=None, author=None
    ):
        """Ingest spans from a given source

        Args:
            layer (str): layer to ingest into
            source (dict): source connection details
            app: a string or a function to infer it from name
            context: a string or a function to infer it from name

        """
        if "gcp" in source:
            bucket = source["gcp"]["bucket"]

            storage_client = storage.Client()
            blobs = storage_client.list_blobs(bucket)

            for blob in blobs:
                yield blob.name


def load():
    """ Go to google cloud, fetch data and turn it into spans

    """
    data = ['lol', 'who', 'dis']

    for d in data:
        yield d

def cache(spans):
    """ If cache exists, return cached spans. If not, read the incoming spans
        and cache them before returning.

    """
    #TODO store cache in a blob
    _cache = ['a']

    if _cache:
        yield from _cache

    else:
        yield from spans

        
if __name__ == "__main__":
    # voices = cache(load())
    voices = pipeline(load, cache)

    voices2 = load()

    print(list(voices))
    print(list(voices2))
