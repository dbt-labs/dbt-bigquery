import pytest
import sys


@pytest.fixture
def remove_agate_from_path():
    original_sys_modules = sys.modules.copy()

    # conftest loads agate modules so we need to remove them
    # and this file
    modules_to_remove = [m for m in sys.modules if "agate" in m]
    for m in modules_to_remove:
        del sys.modules[m]

    yield
    sys.modules = original_sys_modules


def test_lazy_loading_agate(remove_agate_from_path):
    """If agate is imported directly here or in any of the subsequent files, this test will fail. Also test that our assumptions about imports affecting sys modules hold."""
    import dbt.adapters.bigquery.connections
    import dbt.adapters.bigquery.impl
    import dbt.adapters.bigquery.relation_configs._base

    print(dbt.adapters.bigquery.relation_configs._base)
    assert not any([module_name for module_name in sys.modules if "agate" in module_name])

    import agate

    print(agate)
    assert any([module_name for module_name in sys.modules if "agate" in module_name])
