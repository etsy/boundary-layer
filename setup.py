import setuptools
import os

extras = {
        'github': [ 'github3.py>=1.1' ],
        }

long_description = None
with open(os.path.join(os.path.dirname(__file__), 'README.md')) as _in:
    long_description = _in.read()

setuptools.setup(
    name = 'boundary-layer',
    version='1.7.11',
    author = 'Kevin McHale',
    author_email = 'kmchale@etsy.com',
    description = 'Builds Airflow DAGs from configuration files',
    long_description = long_description,
    long_description_content_type = 'text/markdown',
    url = 'https://github.com/etsy/boundary-layer',
    packages = setuptools.find_packages(),
    package_data = {
        'boundary_layer': [
            'builders/templates/*.j2',
            ],
        'boundary_layer_default_plugin': [
            'config/generators/*.yaml',
            'config/operators/*.yaml',
            'config/resources/*.yaml',
            'config/subdags/*.yaml',
            ]
        },
    entry_points = {
            'boundary_layer_plugins': [
                'default=boundary_layer_default_plugin.plugin:DefaultPlugin',
            ]
        },
    extras_require = extras,
    install_requires = [
        'enum34>=1.1.6,<2.0;python_version<"3.4"',
        'semver>=2.7.0,<3.0',
        'jsonschema>=2.6.0,<3.0',
        'jinja2>=2.8.1,<3.0',
        'pyyaml>=4.2b1',
        'marshmallow>=2.13.6,<3.0',
        'networkx>=2.1,<2.3',
        'xmltodict>=0.11.0,<1.0',
        'six>=1.11.0,<2.0',
    ],
    scripts = ['bin/boundary-layer'],
    classifiers = [
        'Development Status :: 5 - Production/Stable',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Topic :: Utilities',
    ],
    license = 'Apache License 2.0',
    keywords = 'airflow',
    zip_safe = False,
    python_requires='>=2.7,!=3.0.*,!=3.1.*,!=3.2.*,!=3.3.*',
)
