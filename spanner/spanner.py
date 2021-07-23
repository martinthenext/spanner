import os
import json
import pprint
from abc import ABC
from datetime import datetime, timedelta
from functools import reduce
from typing import Optional, List, Iterator, Iterable, Callable


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

    def __eq__(self, other):
        if not isinstance(other, Span):
            return False

        return (
            self.app == other.app
            and self.author == other.author
            and self.context == other.context
            and self.start == other.start
        )


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


class AggTextMatch(PipelineStep):
    """Find a string in a plaintext layer."""

    _layer: str
    _query: str

    def __init__(self, layer, query):
        self._layer = layer
        self._query = query.lower()

    def __call__(
        self, layered_spans: Iterable[dict[str, Span]]
    ) -> Iterable[dict[str, Span]]:
        acc_str, acc_list = "", []

        for layered_span in layered_spans:
            text = layered_span[self._layer].content.lower()
            acc_str += text + " "
            acc_list.append(layered_span)

            if self._query in acc_str:
                selected_spans = (
                    ls
                    for ls in acc_list
                    if ls[self._layer].content.lower() in self._query
                )
                yield from selected_spans

                acc_str, acc_list = "", []


class Filter(PipelineStep):
    _func: Callable[dict[str, Span], bool]

    def __init__(self, func):
        self._func = func

    def __call__(
        self, layered_spans: Iterable[dict[str, Span]]
    ) -> Iterable[dict[str, Span]]:
        for ls in layered_spans:
            if self._func(ls):
                yield ls


class Expand(PipelineStep):
    """Expand (start, end) span boundaries for a given layer"""

    _layer: str
    _start_diff: timedelta
    _end_diff: timedelta

    def __init__(self, layer: str, sec_before: float, sec_after: float):
        self._layer = layer
        self._start_diff = timedelta(seconds=sec_before)
        self._end_diff = timedelta(seconds=sec_after)

    def __call__(
        self, layered_spans: Iterable[dict[str, Span]]
    ) -> Iterable[dict[str, Span]]:
        for ls in layered_spans:
            ls[self._layer] = Span(
                start=ls[self._layer].start - self._start_diff,
                end=ls[self._layer].end + self._end_diff,
                app=ls[self._layer].app,
                author=ls[self._layer].author,
                context=ls[self._layer].context,
                # clear out the content
                content=None,
            )
            yield ls


class SelectCutAudio(PipelineStep):
    """Cut audio contained in spans of the audio layer using spans
    of time taken from the cut layer. If cut layer spans intersect,
    merge them.

    This assumes that the cut layer spans are clustered by the audio
    spans, i.e. once you see a new audio span, you have seen all the
    cut spans attributed to it.

    """

    _audio_layer: str
    _cut_layer: str

    def __init__(self, audio_layer: str, by: str):
        self._audio_layer = audio_layer
        self._cut_layer = by

    def merge_cut_spans(self, cut_spans: List[Span]):
        return cut_spans

    def cut_audio(self, audio_span: Span, cut_spans: List[Span]):
        # TODO
        new_audio_file_uri = audio_span.content
        return new_audio_file_uri

    def __call__(
        self, layered_spans: Iterable[dict[str, Span]]
    ) -> Iterable[dict[str, Span]]:

        current_audio_span = None
        acc_cut_spans = []

        for lp in layered_spans:
            if current_audio_span == lp[self._audio_layer]:
                # record a new cut span for the current audio
                acc_cut_spans.append(lp[self._cut_layer])
                continue

            # new audio span detected
            # if there was a previous audio, cut it with accumulated cut spans
            if current_audio_span is not None:
                cut_spans = self.merge_cut_spans(acc_cut_spans)
                audio = self.cut_audio(current_audio_span, cut_spans)
                yield audio

            # change the current span
            current_audio_span = lp[self._audio_layer]
            acc_cut_spans = [lp[self._cut_layer]]

        # ran out of spans, cut the last audio
        cut_spans = self.merge_cut_spans(acc_cut_spans)
        audio = self.cut_audio(current_audio_span, cut_spans)
        yield audio


if __name__ == "__main__":
    QUERY = "hey martin"
    CONTEXTS = ["-595881151"]

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
            Filter(lambda sp: sp["voice"].context in CONTEXTS),
            Cache(
                Convert("voice", "plaintext", "gcp-speech-to-text"),
                storage={
                    "local": STORAGE_DIR,
                    "format": "gcp-speech-api-response",
                    "filename_func": get_json_filename_prefix,
                },
            ),
            AggTextMatch(
                "plaintext",
                QUERY,
            ),
            Expand("plaintext", sec_before=0.3, sec_after=0.2),
            # at this point, "plaintext" spans are still clustered by "voice"
            # spans and, hence, by context
            SelectCutAudio("voice", by="plaintext"),
        ]
    )

    pipeline_computed = list(pipeline)
    # pipeline_computed = [x["plaintext"].content for x in pipeline_computed]
    pp = pprint.PrettyPrinter(indent=2)
    pp.pprint(pipeline_computed[-3:])
