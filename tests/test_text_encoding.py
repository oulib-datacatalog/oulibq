# -*- coding: UTF-8 -*-
from six import ensure_text, PY2


def test_unicode():
    if PY2:
        assert ensure_text("😀") == u"😀"
        assert ensure_text("{0}").format(u"😀") == u"😀"
        assert ensure_text("Testing") == "Testing".decode('utf-8')
        assert ensure_text("Testing") == "Testing"
    else:
        assert ensure_text("😀") == "😀"
        assert ensure_text("{0}").format("😀") == "😀"
