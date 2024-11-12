from typing import Any, Callable, Dict

from dbt_common.clients.jinja import CallableMacroGenerator
from dbt_common.exceptions import DbtInternalError, DbtRuntimeError
from dbt.adapters.contracts.connection import AdapterRequiredConfig
from dbt.adapters.contracts.macros import MacroResolverProtocol
from dbt.adapters.protocol import MacroContextGeneratorCallable


def macro(
    macro_name: str,
    macro_resolver: MacroResolverProtocol,
    macro_context_generator: MacroContextGeneratorCallable,
    config: AdapterRequiredConfig,
    project: str,
    context_override: Dict[str, Any],
) -> Callable:
    """Look macro_name up in the manifest and execute its results.

    :param macro_name: The name of the macro to execute.
    :param macro_resolver: The manifest to use for generating the base macro
        execution context. If none is provided, use the internal manifest.
    :param project: The name of the project to search in, or None for the
        first match.
    :param context_override: An optional dict to update() the macro
        execution context.
    """
    if macro_resolver is None:
        raise DbtInternalError("Macro resolver was None when calling execute_macro!")

    if macro_context_generator is None:
        raise DbtInternalError("Macro context generator was None when calling execute_macro!")

    macro = macro_resolver.find_macro_by_name(macro_name, config.project_name, project)
    if macro is None:
        if project is None:
            package_name = "any package"
        else:
            package_name = 'the "{}" package'.format(project)

        raise DbtRuntimeError(
            'dbt could not find a macro with the name "{}" in {}'.format(macro_name, package_name)
        )

    macro_context = macro_context_generator(macro, config, macro_resolver, project)
    macro_context.update(context_override)

    return CallableMacroGenerator(macro, macro_context)
