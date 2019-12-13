from urllib.parse import parse_qsl


class FakeClient(object):

    def __init__(self):
        self._in_list = []
        self._out_list = []
        self._out_index = 0

    def post(self, url, headers=None, data=None, *args, **kwargs):
        rv = {
            'method': 'POST',
            'url': url,
        }
        if headers is not None:
            rv['headers'] = headers
        if data is not None:
            rv['data'] = parse_qsl(data)
        self._in_list.append(rv)

        rv = self._out_list[self._out_index]
        self._out_index += 1
        return FakeResponse(rv)

    def reset(self):
        self._in_list = []
        self._out_list = []
        self._out_index = 0

    def add_json(self, dict_):
        self._out_list.append(dict_)

    def get_request_sequence(self):
        return self._in_list


class FakeResponse(object):

    def __init__(self, data):
        self._data = data

    async def __aenter__(self):
        return self

    async def __aexit__(self, et, ev, tb):
        pass

    def raise_for_status(self):
        pass

    async def json(self):
        return self._data
