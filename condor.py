from parsl.config import Config
from parsl.monitoring.monitoring import MonitoringHub
from parsl.providers import CondorProvider
from parsl.providers import LocalProvider
from parsl.executors import HighThroughputExecutor
from parsl.addresses import address_by_hostname


def get_config(key, env_path):
    """
    Creates an instance of the Parsl configuration 
    """

    executors = {
        "htcondor": HighThroughputExecutor(
            label='htcondor',
            address=address_by_hostname(),
            max_workers=2,
            provider=CondorProvider(
                init_blocks=10,
                min_blocks=10,
                max_blocks=10,
                requirements='((Machine == "apl05.ib0.cm.linea.gov.br")||(Machine == "apl06.ib0.cm.linea.gov.br")||(Machine == "apl07.ib0.cm.linea.gov.br")||(Machine == "apl08.ib0.cm.linea.gov.br")||(Machine == "apl09.ib0.cm.linea.gov.br")||(Machine == "apl10.ib0.cm.linea.gov.br")||(Machine == "apl11.ib0.cm.linea.gov.br")||(Machine == "apl12.ib0.cm.linea.gov.br")||(Machine == "apl13.ib0.cm.linea.gov.br")||(Machine == "apl14.ib0.cm.linea.gov.br")||(Machine == "apl15.ib0.cm.linea.gov.br")||(Machine == "apl16.ib0.cm.linea.gov.br"))',
                scheduler_options='+RequiresWholeMachine = True',
                worker_init=f"source {env_path}",
                cmd_timeout=120,
            ),
        ),
        "local": HighThroughputExecutor(
            label='local',
            provider=LocalProvider(
                min_blocks=1,
                init_blocks=1,
                max_blocks=2,
                nodes_per_block=1
            )
        )
    }

    executor = executors[key]

    return Config(
        executors=[executor],
        strategy=None
    )
