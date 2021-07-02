from google.cloud import storage
from functools import reduce
from typing import Optional, List, Iterator, Iterable


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


class PipelineStep:
    def __init__(self, **kwargs):
        self._params = kwargs


def chain(steps: List[PipelineStep]) -> Iterable[Span]:
    return reduce(lambda x, y: y(x), steps)


class Load(PipelineStep):
    """ Generating the data rather than processing, Load is an
        iterable

    """
    def __iter__(self) -> Iterator[Span]:
        """Go to google cloud, fetch data and turn it into spans"""
        data = ["lol", "who", "dis"]

        for d in data:
            yield d


class Cache(PipelineStep):
    _cache = None

    def __call__(self, spans):
        """If cache exists, return cached spans. If not, read the incoming spans
        and cache them before returning.

        """
        # TODO store cache in a blob
        if self._params.get("backend") == "file":
            self._cache = ["a"]

        if self._cache:
            yield from self._cache

        else:
            yield from spans


if __name__ == "__main__":
    pipeline = chain(
        [
            Load(storage={"gcp": {"bucket": "a"}, "suffix": ".oga"}),
            Cache(backend="file"),
        ]
    )
    print(list(pipeline))
