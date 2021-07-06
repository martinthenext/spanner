import datetime


"""
This script does the following:
- define Span class and helper functions
- generate data: iterator of dictionaries with keys 'text' and 'voice'. Both values are a span (single word). Each has an absolute start/end time
- filter stream based on query
- sort by start time (in case they're not sorted)
- merge spans that overlap (give some window in ms)

You can change the wibdow to see how different spans are merged

Assumption: all the filtered spans are by the same author in the same app.

So to do the general case (ie: different authors, or different messages by the same author), do:
- after filtering stream based on query, separate the results into several lists (one for each message/author/app)
- Return a list of merged spans for each of these messages/author/app
- We'll then cut the relevant audio files and return several audio files to the user
"""

class Span:
    # id
    app: str
    author: str
    context: str
    start: int
    end: int

    # data
    content: str  # text for plaintext, URI for everything else

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    def __repr__(self):
        return str(self.__dict__)


# =======
def is_overlap(s1, e1, s2, e2, window_ms=0):
    """Checks whether 2 sets of datetimes overlap.
    window (ms): adds a window to both sides of the 2 intervals
    """
    latest_start = max(s1, s2)
    earliest_end = min(e1, e2)
    delta = earliest_end - latest_start + datetime.timedelta(milliseconds=2*window_ms)
    td_0 = datetime.timedelta(0)
    return delta>=td_0

# tests:
def_time = lambda s : datetime.datetime(2021, 7, 5, 12, 22, s)
assert not is_overlap(def_time(0), def_time(3), def_time(5), def_time(6), 500)
assert is_overlap(def_time(0), def_time(3), def_time(4), def_time(6), 500)
assert not is_overlap(def_time(0), def_time(3), def_time(4), def_time(6), 0)
assert not is_overlap(def_time(0), def_time(3), def_time(5), def_time(6), 500)
assert is_overlap(def_time(0), def_time(3), def_time(5), def_time(6), 1000)
assert is_overlap(def_time(0), def_time(3), def_time(1), def_time(2), 500)

# =======


def filter_spans(stream, query):
    accumulator = ''
    accumulator_list = []
    for elem in stream:
        x = elem['text'].content
        accumulator += x + ' '
        accumulator_list.append(elem)
        if query in accumulator:
            yield from (e for e in accumulator_list if e['text'].content in query)
            accumulator = ''
            accumulator_list = []

def merge_spans(sorted_f_list, window_ms=500):
    """
    sorted_f_list: list
        Each element is a dict with keys 'text' and 'voice'. Both values are a span (single word)
        These are sorted by start time
    """
    # initialise the accumulator
    accumulator = {'start': sorted_f_list[0]['text'].start, "content": ""}
    merged_span_list = []

    for idx in range(0, len(sorted_f_list)-1):
        accumulator['content'] += " " + sorted_f_list[idx]['text'].content
        s1, e1 = sorted_f_list[idx]['text'].start, sorted_f_list[idx]['text'].end
        s2, e2 = sorted_f_list[idx+1]['text'].start, sorted_f_list[idx+1]['text'].end
        if is_overlap(s1,e1,s2,e2, window_ms):
            pass
        else:
            accumulator['end'] = e1
            new_span = Span(**accumulator)
            merged_span_list.append({'text': new_span, 'voice': new_span})
            accumulator = {'start': sorted_f_list[idx]['text'].start, "content": ""}

    # append the final span
    accumulator['content'] += " " + sorted_f_list[-1]['text'].content
    accumulator['end'] = sorted_f_list[-1]['text'].end
    new_span = Span(**accumulator)
    merged_span_list.append({'text': new_span, 'voice': new_span})
    return merged_span_list

# =======
# =======


# define fake data
# We assume that spans have the same details (author, app, voice message..)
words_iter = iter(["lala", "my", "name", 'hello', 'my', 'name', 'is',  "bob"])
def_time = lambda s : datetime.datetime(2021, 7, 5, 12, 22, s)
word_times = [(def_time(0), def_time(1)), # lala ==== span 1
            (def_time(4), def_time(5)), # my ==== span 2
            (def_time(5), def_time(6)), # name
            (def_time(6), def_time(7)), # hello
            (def_time(8), def_time(9)), # my
            (def_time(10), def_time(11)), # name
            (def_time(14), def_time(15)), # is ===== span 3
            (def_time(17), def_time(18)) # bob
]

stream_spans = ({
                    "text": Span(content=elem, start=t[0], end=t[1]),
                    "voice": Span(content=elem, start=t[0], end=t[1])
                }
                     for elem, t in zip(words_iter, word_times))
# =======
# =======

# filter stream based on query
filtered_list = list(filter_spans(stream_spans, "my name"))
# print(filtered_list)

print("Filtered stream: ", [e['text'].content for e in filtered_list])
# print("\n")


# sort by start time
sorted_f_list = sorted(filtered_list, key = lambda s: s['text'].start)
# print(sorted_f_list)

# bool list saying whether or not pairs of consecutive spans overlap
bool_list = []
for idx in range(0, len(sorted_f_list)-1):
    s1, e1 = sorted_f_list[idx]['text'].start, sorted_f_list[idx]['text'].end
    s2, e2 = sorted_f_list[idx+1]['text'].start, sorted_f_list[idx+1]['text'].end
    bool_list.append(is_overlap(s1,e1,s2,e2))


# merge
merged_span_list = merge_spans(sorted_f_list, window_ms=500)
# print("\nMerged spans:\n", merged_span_list)
print("\nMerged spans:\n", [e['text'].content for e in merged_span_list])
