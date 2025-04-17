DEFAULT_FLOW_FMT = '%(asctime)s.%(msecs)03d | %(levelname)-7s | Flow run %(flow_run_name)r - %(message)s'
DEFAULT_TASK_FMT = '%(asctime)s.%(msecs)03d | %(levelname)-7s | Task run %(task_run_name)r - %(message)s'

# The default logging configuration obtained from Prefect
PREFECT_DEFAULT_LOG_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'simple': {
            'format': '%(asctime)s.%(msecs)03d | %(message)s',
            'datefmt': '%H:%M:%S'
        },
        'standard': {
            '()': 'prefect.logging.formatters.PrefectFormatter',
            'format': '%(asctime)s.%(msecs)03d | %(levelname)-7s | %(name)s - %(message)s',
            'flow_run_fmt': '%(asctime)s.%(msecs)03d | %(levelname)-7s | Flow run %(flow_run_name)r - %(message)s',
            'task_run_fmt': '%(asctime)s.%(msecs)03d | %(levelname)-7s | Task run %(task_run_name)r - %(message)s',
            'datefmt': '%H:%M:%S'
        },
        'debug': {
            'format': '%(asctime)s.%(msecs)03d | %(levelname)-7s | %(threadName)-12s | %(name)s - %(message)s',
            'datefmt': '%H:%M:%S'
        },
        'json': {
            'class': 'prefect.logging.formatters.JsonFormatter',
            'format': 'default'
        }
    },
    'handlers': {
        'console': {
            'level': 0,
            'class': 'prefect.logging.handlers.PrefectConsoleHandler',
            'formatter': 'standard',
            'styles': {
                'log.web_url': 'bright_blue',
                'log.local_url': 'bright_blue',
                'log.info_level': 'cyan',
                'log.warning_level': 'yellow3',
                'log.error_level': 'red3',
                'log.critical_level': 'bright_red',
                'log.completed_state': 'green',
                'log.cancelled_state': 'yellow3',
                'log.failed_state': 'red3',
                'log.crashed_state': 'bright_red',
                'log.flow_run_name': 'magenta',
                'log.flow_name': 'bold magenta'
            }
        },
        'api': {
            'level': 0,
            'class': 'prefect.logging.handlers.APILogHandler'
        },
        'debug': {
            'level': 0,
            'class': 'logging.StreamHandler',
            'formatter': 'debug'
        }
    },
    'loggers': {
        'prefect': {
            'level': 'INFO'
        },
        'prefect.extra': {
            'level': 'INFO',
            'handlers': ['api']
        },
        'prefect.flow_runs': {
            'level': 'INFO',
            'handlers': ['api']
        },
        'prefect.task_runs': {
            'level': 'NOTSET',
            'handlers': ['api']
        },
        'prefect.server': {
            'level': 'WARNING'
        },
        'prefect.client': {
            'level': 'INFO'
        },
        'prefect.infrastructure': {
            'level': 'INFO'
        },
        'prefect._internal': {
            'level': 'ERROR',
            'propagate': False,
            'handlers': ['debug']
        },
        'uvicorn': {
            'level': 'WARNING'
        },
        'fastapi': {
            'level': 'WARNING'
        }
    },
    'root': {
        'level': 'WARNING',
        'handlers': ['console']
    },
    'incremental': True
}
