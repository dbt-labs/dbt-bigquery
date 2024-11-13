from typing import Any, Callable, Dict, Optional

from dbt_common.clients.jinja import CallableMacroGenerator
from dbt_common.exceptions import DbtInternalError, DbtRuntimeError
from dbt_common.utils.dict import AttrDict
from dbt.adapters.contracts.connection import AdapterRequiredConfig
from dbt.adapters.contracts.macros import MacroResolverProtocol
from dbt.adapters.protocol import MacroContextGeneratorCallable

from dbt.adapters.bigquery.relation import BigQueryRelation


class MacroService:
    def __init__(
        self,
        macro_resolver: MacroResolverProtocol,
        macro_context_generator: MacroContextGeneratorCallable,
        config: AdapterRequiredConfig,
    ) -> None:
        if macro_resolver is None:
            raise DbtInternalError("Macro resolver was None when calling execute_macro!")

        if macro_context_generator is None:
            raise DbtInternalError("Macro context generator was None when calling execute_macro!")

        self._macro_resolver = macro_resolver
        self._macro_context_generator = macro_context_generator
        self._config = config
        self._root_project = config.project_name

    def create_schema(self, relation: BigQueryRelation) -> AttrDict:
        create_schema = self.macro("create_schema")
        return create_schema({"schema": relation.without_identifier()})

    def macro(
        self,
        macro_name: str,
        package: Optional[str] = None,
        context_override: Optional[Dict[str, Any]] = None,
    ) -> Callable:
        if macro := self._macro_resolver.find_macro_by_name(
            macro_name, self._root_project, package
        ):
            macro_context = self._macro_context_generator(
                macro, self._config, self._macro_resolver, package
            )
            macro_context.update(context_override)
        else:
            self._handle_missing_macro(macro_name, package)
        return CallableMacroGenerator(macro, macro_context)

    @staticmethod
    def _handle_missing_macro(macro_name: str, package: Optional[str]) -> None:
        if package is None:
            msg = f'dbt could not find a macro with the name "{macro_name}" in any package'
        else:
            msg = f'dbt could not find a macro with the name "{macro_name}" in the "{package}" package'

        raise DbtRuntimeError(msg)
