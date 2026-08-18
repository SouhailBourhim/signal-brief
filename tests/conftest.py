from __future__ import annotations

import pytest

from signal_core.config import SOURCES
from signal_core.contracts import State
from signal_core.sources import get_poller


@pytest.fixture
def fake_config():
    return SOURCES["fake"]


@pytest.fixture
def polled(fake_config):
    return get_poller("fake")(fake_config, State(source_id="fake"))
