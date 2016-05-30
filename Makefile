all: build

build:
	python2 setup.py build
	python3 setup.py build

docs:
	sphinx-build -M html docs/ build/

spelling:
	sphinx-build -M spelling docs/ build/

clean:
	-rm -rf build/

realclean: clean
	-rm -rf baseplate.egg-info/

tests:
	nosetests
	nosetests3
	sphinx-build -M doctest docs/ build/
	sphinx-build -M spelling docs/ build/

develop:
	python2 setup.py develop
	python3 setup.py develop

install:
	python2 setup.py install
	python3 setup.py install

lint:
	pep8 --repeat --ignore=E501,E128,E226 --exclude=baseplate/thrift/ baseplate/
	pylint --errors-only baseplate/


.PHONY: docs spelling clean realclean tests develop install build lint
