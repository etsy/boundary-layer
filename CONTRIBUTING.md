# Contribute
We welcome contributions from the Airflow community!

There are several types of contributions for which we will accept pull requests without prior communication:
1. Addition of support for currently-unsupported Airflow [operators](boundary_layer_default_plugin/config/operators)
2. Updates to documentation
3. Addition of tests
4. Bug fixes, assuming steps are provided to reproduce the bug

For any other type of change, please submit an [issue](https://github.com/etsy/boundary-layer/issues) so that we can discuss before you go ahead and implement anything.

## Submitting contributions

These are the steps for submitting a proposed contribution to boundary-layer:

1. Create an [issue](https://github.com/etsy/boundary-layer/issues) unless your change falls into one of the categories listed above
2. Fork from [the master branch](https://github.com/etsy/boundary-layer)
3. Make your changes
4. Document new functionality as appropriate
5. Add new tests if possible
6. [Recommended] Run the test suite locally with `tox --recreate test`
7. Push your changes to your fork
8. Send a pull request!

Every pull request will be tested by Github Actions. The CI build runs unit tests, [pycodestyle](https://pypi.org/project/pycodestyle/) and [pylint](https://www.pylint.org/).  All tests must pass, including the code style and linter tests.

# Contributors
- Kevin McHale [mchalek](https://github.com/mchalek)
- Jessica Fan [jfantom](https://github.com/jfantom)
- Aaron Niskode-Dossett [dossett](https://github.com/dossett)
- Nikki Thean [nixsticks](https://github.com/nixsticks)
- Julie Chien [jj-ian](https://github.com/jj-ian)
- Andrey Polyakov [andpol](https://github.com/andpol)
- Sahil Khanna [SahilKhanna129](https://github.com/SahilKhanna129)
- Kiril Zvezdarov [kzvezdarov](https://github.com/kzvezdarov)
- Hyun Goo Kang [hyungoos](https://github.com/hyungoos)
- Nick Sawyer [nickmoorman](https://github.com/nickmoorman)
