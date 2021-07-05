
class Span:
    # id
    layer: str
    app: str
    author: str
    context: str
    start: int

    # data
    content: str

    def __init__(self, content):
        self.content = content

lala = Span(content="hello")
print(lala.content)

words_iter = iter(["lal", "my", "name", 'hello', 'my', 'name', 'is',  "bob"])
stream_spans = ({"text": Span(content=elem), "voice": Span(content=elem)} for elem in words_iter)


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

mylist = list(filter_spans(stream_spans, "my name"))
print(mylist)
print([e['text'].content for e in mylist])
