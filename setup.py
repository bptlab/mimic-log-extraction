from setuptools import setup, find_packages  # type: ignore


requirements = [
    'argparse==1.4.0',
    'numpy==1.22.2',
    'matplotlib==3.5.1',
    'pandas==1.4.1',
    'pandasql==0.7.3',
    'pm4py==2.2.19.1',
    'psycopg2==2.9.3',
    'scikit-learn==1.0.2',
    'scipy==1.8.0',
    'SQLAlchemy==1.4.31',
    'python-dotenv==0.19.2'
]


setup(
    name='mimic-extraction',
    version='0.1',
    description='Extraction tool for event logs out of Mimic IV Data Sets',
    url='https://gitlab.hpi.de/finn.klessascheck/mimic-log-extraction-tool',
    author='Finn Klessascheck, Jonas Cremerius',
    author_email='finn.klessascheck@student.hpi.de, jonas.cremerius@hpi.de',
    keywords='xes mimic event',
    packages=find_packages(),
    install_requires=requirements,
    extras_require={
        'dev': [
            'typing_extensions==4.1.1',
            'mypy==0.940',
            'mypy-extensions==0.4.3',
            'pandas-stubs==1.2.0.50',
            'data-science-types==0.2.23',
            'pylint==2.12.2',
            'types-psycopg2==2.9.8 '
        ]
    },
    include_package_data=True,
)
