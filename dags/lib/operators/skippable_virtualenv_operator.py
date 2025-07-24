from typing import Callable, Optional, Union, Iterable, List, Collection, Any, Mapping, Container
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonVirtualenvOperator


class SkippablePythonVirtualenvOperator(PythonVirtualenvOperator):
    """
    Custom PythonVirtualenvOperator that allows skipping a task.
    """

    template_fields = PythonVirtualenvOperator.template_fields + (
        'skip',
    )

    def __init__(self,
                 *,
                 python_callable: Callable,
                 requirements: Optional[Union[Iterable[str], str]] = None,
                 python_version: Optional[Union[str, int, float]] = None,
                 use_dill: bool = False,
                 system_site_packages: bool = True,
                 pip_install_options: Optional[List[str]] = None,
                 op_args: Optional[Collection[Any]] = None,
                 op_kwargs: Optional[Mapping[str, Any]] = None,
                 string_args: Optional[Iterable[str]] = None,
                 templates_dict: Optional[dict] = None,
                 templates_exts: Optional[List[str]] = None,
                 expect_airflow: bool = True,
                 skip_on_exit_code: Optional[Union[int, Container[int]]] = None,
                 skip: bool = False,
                 **kwargs,):
        super().__init__(
            python_callable=python_callable,
            requirements=requirements,
            python_version=python_version,
            use_dill=use_dill,
            system_site_packages=system_site_packages,
            pip_install_options=pip_install_options,
            op_args=op_args,
            op_kwargs=op_kwargs,
            string_args=string_args,
            templates_dict=templates_dict,
            templates_exts=templates_exts,
            expect_airflow=expect_airflow,
            skip_on_exit_code=skip_on_exit_code,
            **kwargs)

        self.skip = skip

    def execute(self, **kwargs):
        if self.skip:
            raise AirflowSkipException("Skipping PythonVirtualenvOperator.")

        return super().execute(**kwargs)



