from setuptools import find_packages, setup

setup(
    name='skygear_event_tracking',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        'skygear>=0.21.0',
        'SQLAlchemy>=1.0.0',
        'alembic>=0.8.0',
        'requests>=2.0.0',
    ],
    test_suite='skygear_event_tracking.tests',
)
