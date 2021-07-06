import os
import json
import pprint
from abc import ABC
from datetime import datetime, timedelta
from functools import reduce
from typing import Optional, List, Iterator, Iterable


STORAGE_DIR = "/Users/martin/voices-bot-data/voices-bot-data-new"
TS_FORMAT = "%Y-%m-%d_%H:%M:%S.%f"


class Span:
    # id
    app: str
    author: str
    context: str
    start: datetime

    # data
    content: str  # text for plaintext, URI for everything else

    # optional
    end: Optional[datetime]

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    def __repr__(self):
        return str(self.__dict__)


class PipelineStep(ABC):
    """Pipeline steps get inialized with input data source and act as
    iterators. On each iteration they will read input, perform
    computation on it and yield results.

    Steps both consume and produce iterables of so layered spans:

    [ { layer1: Span(...), layer2: Span(...) }, ... ]

    """

    def __call__(
        self, layered_spans: Iterable[dict[str, Span]]
    ) -> Iterable[dict[str, Span]]:
        pass


def chain(steps: List[PipelineStep]) -> Iterable[dict]:
    return reduce(lambda x, y: y(x), steps)


def get_timestamp_app_author_context(filename):
    app = "telegram"
    parts = filename.split("-")
    ts = datetime.strptime("-".join(parts[:3]), TS_FORMAT)
    author = parts[3]
    context = parts[4] if parts[4] else "-" + parts[5]
    return ts, app, author, context


def get_json_filename_prefix(timestamp, context, author):
    ts_str = timestamp.strftime(TS_FORMAT)
    prefix = f"{ts_str}-{author}-{context}"

    return prefix


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

    def __call__(self, spans) -> Iterable[dict[str, Span]]:
        """Call the external API to compute possibly multiple converted
        spans for each input spans
        """
        for span in spans:
            raise NotImplementedError(
                "External API is not available, please " "use cache instead"
            )


class Cache:
    """Cache the given Convert step. Process spans from the input layer:

        - If converted spans are available in the cache, yield them
        - Otherwise, yield converted spans from the Convert step

    If storage is local, and format is "gcp-speech-api-response",
    it will look for a standard GCP response JSON for a given input
    layer span under the filename produced by the <get_json_filename_prefix>
    function.

    """

    _storage = None
    _convert_step = None

    def __init__(self, convert_step: PipelineStep, storage: dict):
        self._storage = storage
        self._convert_step = convert_step

    def __call__(
        self, layered_spans: Iterable[dict[str, Span]]
    ) -> Iterable[dict[str, Span]]:
        storage_dir = self._storage.get("local")
        filename_func = self._storage.get("filename_func")

        for layered_span in layered_spans:
            input_span = layered_span[self._convert_step.input_layer]
            filename_prefix = filename_func(
                input_span.start, input_span.context, input_span.author
            )
            filenames = [
                f
                for f in os.listdir(storage_dir)
                if f.startswith(filename_prefix) and f.endswith(".json")
            ]
            filename = filenames.pop() if filenames else None

            if filename:
                # cache found for this input span, return it
                path = os.path.join(storage_dir, filename)
                output_spans = self.get_cashed_output_spans(path, input_span)

                # layer output spans on top of the existing input
                layered_spans = (
                    {self._convert_step.output_layer: output_span, **layered_span}
                    for output_span in output_spans
                )

                yield from layered_spans
            else:

                # cache not found for this span, call the convert
                yield from self._convert_step(input_span)

    def get_cashed_output_spans(self, output_spans_path, input_span):
        """Given the input span, get corresponding output spans
        from the path and yield layered spans.

        """
        if self._storage.get("format") != "gcp-speech-api-response":
            raise NotImplementedError(
                "Only caching GCP Speech API " "response JSONs is implemented"
            )

        with open(output_spans_path) as f:
            output_spans = json.load(f)

        result = next(iter(output_spans.get("results", [])), {})
        alternative = next(iter(result.get("alternatives", [])), {})

        confidence = alternative.get("confidence")
        for word in alternative.get("words", []):
            output_span_content = word["word"]

            start_seconds = float(word["startTime"].replace("s", ""))
            output_span_start = input_span.start + timedelta(seconds=start_seconds)

            end_seconds = float(word["endTime"].replace("s", ""))
            output_span_end = input_span.start + timedelta(seconds=end_seconds)

            output_span = Span(
                app=input_span.app,
                author=input_span.author,
                context=input_span.context,
                start=output_span_start,
                end=output_span_end,
                content=output_span_content,
            )

            yield output_span


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
            Cache(
                Convert("voice", "plaintext", "gcp-speech-to-text"),
                storage={
                    "local": STORAGE_DIR,
                    "format": "gcp-speech-api-response",
                    "filename_func": get_json_filename_prefix,
                },
            ),
        ]
    )

    pipeline_computed = list(pipeline)
    pp = pprint.PrettyPrinter(indent=2)
    pp.pprint(pipeline_computed[-3:])
