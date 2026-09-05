"""Shared pytest fixtures for yard-plugin-airflow protocol tests.

Provides subprocess-spawning helpers that mirror the Rust assert_cmd patterns
used in yard-plugin-glue/tests/protocol.rs.
"""

import json
import os
import subprocess

import pytest


# Absolute path to the plugin script
PLUGIN_SCRIPT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "yard-plugin-airflow.py",
)


@pytest.fixture
def plugin_script():
    """Return the absolute path to yard-plugin-airflow.py."""
    return PLUGIN_SCRIPT


@pytest.fixture
def run_plugin_fn():
    """Return a callable that spawns the plugin with a JSON request dict.

    Usage in tests::

        def test_something(run_plugin_fn):
            stdout, stderr, rc = run_plugin_fn({"operation": "schema"})
    """

    def _run(request_dict):
        input_data = json.dumps(request_dict) + "\n"
        result = subprocess.run(
            ["python3", PLUGIN_SCRIPT],
            input=input_data,
            capture_output=True,
            text=True,
            timeout=10,
        )
        return result.stdout, result.stderr, result.returncode

    return _run
