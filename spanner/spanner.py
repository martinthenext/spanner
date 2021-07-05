import os
from datetime import datetime
from google.cloud import storage
from functools import reduce
from typing import Optional, List, Iterator, Iterable


STORAGE_DIR = "/Users/martin/voices-bot-data/voices-bot-data-new"


class Span:
    # id
    app: str
    author: str
    context: str
    start: int

    # data
    content: str  # text for plaintext, URI for everything else

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    def __repr__(self):
        return str(self.__dict__)


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
    pass


def chain(steps: List[PipelineStep]) -> Iterable[Span]:
    return reduce(lambda x, y: y(x), steps)


def get_timestamp_app_author_context(filename):
    app = "telegram"
    parts = filename.split("-")
    ts = datetime.strptime("-".join(parts[:3]), "%Y-%m-%d_%H:%M:%S.%f")
    author = parts[3]
    context = parts[4] if parts[4] else "-" + parts[5]
    return ts, app, author, context


class Load(PipelineStep):
    """Generating the data rather than processing, Load is an
    iterable

    """

    _layer = None
    _storage = None

    def __init__(self, layer, storage):
        self._layer = layer
        self._storage = storage

    def __iter__(self) -> Iterator[Span]:
        """Go to google cloud, fetch data and turn it into spans"""
        if "local" in self._storage:
            storage_dir = self._storage["local"]
            suffix = self._storage.get("suffix", "ogg")
            metadata_func = self._storage.get("metadata_func")

            for filename in os.listdir(storage_dir):
                if filename.endswith(suffix):
                    ts, app, author, context = metadata_func(filename)
                    content_uri = f"file://{storage_dir}/{filename}"

                    yield {
                        self._layer: Span(
                            app=app,
                            author=author,
                            context=context,
                            start=ts,
                            content=content_uri,
                        )
                    }


class Convert(PipelineStep):
    """Convert data from one modality to another one using an
    external API

    """

    input_layer: str
    output_layer: str
    api: str

    def __init__(self, input_layer, output_layer, api):
        self.input_layer = input_layer
        self.output_layer = output_layer
        self.api = api

    def __call__(self, spans):
        yield from spans


class Cache(PipelineStep):
    _cache = None
    _layer = None
    _backend = None

    def __init__(self, layer, backend):
        self._layer = layer
        self._backend = backend

    def __call__(self, spans):
        """If cache exists, return cached spans. If not, read the incoming spans
        and cache them before returning.

        """
        # TODO store cache in a blob
        if self._backend == "file":
            self._cache = ["a"]

        if self._cache:
            yield from self._cache

        else:
            yield from spans


if __name__ == "__main__":
    pipeline = chain(
        [
            Load(
                "voice",
                storage={
                    "local": STORAGE_DIR,
                    "suffix": ".oga",
                    "metadata_func": get_timestamp_app_author_context,
                },
            ),
            Convert("voice", "plaintext", "gcp-speech-to-text"),
            Cache("plaintext", backend="none"),
        ]
    )
    print(list(pipeline))
