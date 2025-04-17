import logging

from prefect.logging.configuration import setup_logging as setup_logging_prefect

def configure_custom_logging(var: str) -> None:
    """
    Configures Prefect logging to include a custom variable in every log message.
    Args:
        var (str): The variable to include in log messages.
    """
    class VarFilter(logging.Filter):
        def filter(self, record):
            record.var = var
            return True

    cfg = setup_logging_prefect(incremental=True)

    cfg['filters'] = {
        'var_filter': {
            '()': VarFilter, # class
            # 'arg1': 'val1', # ex passing args to the class
            # 'arg2': 'val2', # ex passing args to the class
        },
    }

    for name, fmts in cfg['formatters'].items():
        if name == 'standard':
            cfg['formatters'][name]['datefmt'] = '%d-%m-%yT%H:%M:%S'
            if 'flow_run_fmt' in cfg['formatters'][name]:
                if 'var' not in cfg['formatters'][name]['flow_run_fmt']:
                    cfg['formatters'][name]['flow_run_fmt'] = cfg['formatters'][name]['flow_run_fmt'].replace('%(levelname)-7s', '%(levelname)s | %(var)s')
                    cfg['formatters'][name]['flow_run_fmt'] = cfg['formatters'][name]['flow_run_fmt'].replace('%(asctime)s.%(msecs)03d', '%(asctime)s')
            if 'task_run_fmt' in cfg['formatters'][name]:
                if 'var' not in cfg['formatters'][name]['task_run_fmt']:
                    cfg['formatters'][name]['task_run_fmt'] = cfg['formatters'][name]['task_run_fmt'].replace('%(levelname)-7s', '%(levelname)s | %(var)s')
                    cfg['formatters'][name]['task_run_fmt'] = cfg['formatters'][name]['task_run_fmt'].replace('%(asctime)s.%(msecs)03d', '%(asctime)s')

    for name, settings in cfg['handlers'].items():
        if 'filters' not in cfg['handlers'][name]:
            cfg['handlers'][name]['filters'] = ['var_filter']
        else:
            cfg['handlers'][name]['filters'].append('var_filter')

    cfg['incremental'] = False
    logging.config.dictConfig(cfg)
