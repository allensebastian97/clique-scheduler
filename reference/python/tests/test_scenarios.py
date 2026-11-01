"""The reference implementation must keep producing the shared scenarios' results."""
import json

import pytest

from tools.scenarios import SCENARIOS, run

FILES = sorted(SCENARIOS.glob('*.json'))


def test_scenarios_exist():
    assert FILES, f'no scenarios in {SCENARIOS}'


@pytest.mark.parametrize('path', FILES, ids=[p.stem for p in FILES])
def test_scenario(path):
    spec = json.loads(path.read_text())
    assert run(spec) == spec['expect']
