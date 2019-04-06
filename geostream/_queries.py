
# unfinished experiments with linked queries

def lquery(self, query):
    return QuerySet(self.stream.c, [query])





class QuerySet(object):
    def __init__(self, cursor, queries):
        self.cursor = cursor
        self.queries = queries

    def query(self, query):
        return QuerySet(self.cursor, self.queries+[query])

    def construct_query(self):
        # most recent first
        def wrap(query, prevqueries):
            return '{} ({})'.format(query, prevqueries)
        querygen = (q for q in self.queries)
        cumul = next(querygen)
        nxt = next(querygen, None)
        while nxt:
            cumul = wrap(nxt, cumul)
            nxt = next(querygen, None)
            print cumul
        print cumul
        return cumul

    def results(self):
        final_query = self.construct_query()
        return self.cursor.execute(final_query)
