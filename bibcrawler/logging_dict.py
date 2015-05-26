#!/usr/bin/env python
# -*- coding: utf-8 -*-

def logging_confdict(working_dir, name):
    logging_dict = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'fileFormatter': {
                'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            },
            'consoleFormatter': {
                'format': '%(message)s'
            }
        },
        'handlers': {
            'fileHandler': {
                'class': 'logging.FileHandler',
                'level': 'INFO',
                'formatter': 'fileFormatter',
                'filename': working_dir + "/" + name + '.log',
            },
            'consoleHandler': {
                'level': 'DEBUG',
                'class': 'logging.StreamHandler',
                'formatter': 'consoleFormatter'
            }

        },
        'loggers': {
            name: {
                'handlers': ['fileHandler', 'consoleHandler'],
                'level': 'DEBUG',
            }
        }
    }

    return logging_dict