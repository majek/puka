CODEGEN_DIR=vendor/rabbitmq-codegen
AMQP_JSON_SPEC=$(CODEGEN_DIR)/amqp-rabbitmq-0.9.1.json

PYTHON=python

all: vendor/rabbitmq-codegen/amqp_codegen.py puka/spec.py puka/spec_exceptions.py tests

vendor/rabbitmq-codegen/amqp_codegen.py:
	git submodule update --init

$(AMQP_JSON_SPEC):
	@echo "You need '$(CODEGEN_DIR)' package."
	@echo "Try one of the following:"
	@echo "  git submodule init && git submodule update"
	@exit 1

puka/spec.py: codegen.py codegen_helpers.py \
		$(CODEGEN_DIR)/amqp_codegen.py \
		$(AMQP_JSON_SPEC) amqp-accepted-by-update.json
	$(PYTHON) codegen.py spec $(AMQP_JSON_SPEC) puka/spec.py

puka/spec_exceptions.py: codegen.py codegen_helpers.py \
		$(CODEGEN_DIR)/amqp_codegen.py \
		$(AMQP_JSON_SPEC) amqp-accepted-by-update.json
	$(PYTHON) codegen.py spec_exceptions $(AMQP_JSON_SPEC) puka/spec_exceptions.py

clean:
	find . -name \*pyc|xargs --no-run-if-empty rm
	rm -f tests/.coverage distribute-0.6.10.tar.gz
	rm -rf venv

distclean: clean
	rm -f puka/spec.py puka/spec_exceptions.py
	rm -rf build dist puka.egg-info

.PHONY: tests prerequisites

TEST_AMQP_URL:=amqp://127.0.0.1/

test: tests
tests: puka/spec.py
	cd tests && AMQP_URL=$(TEST_AMQP_URL) PYTHONPATH=.. $(PYTHON) tests.py ../puka puka puka.urlparse


venv:
	virtualenv --no-site-packages venv
	./venv/bin/easy_install coverage
	./venv/bin/easy_install nose
	./venv/bin/easy_install sphinx

DOCS=../puka-gh-pages
$(DOCS):
	mkdir $(DOCS)
	git clone git@github.com:majek/puka.git $(DOCS)
	cd $(DOCS) && \
		git branch -f gh-pages origin/gh-pages && \
		git checkout gh-pages

generate-docs: $(DOCS) venv
	cd $(DOCS) && git pull
	./venv/bin/sphinx-build -b html docs $(DOCS)
	echo '<meta http-equiv="refresh" content="0;url=./puka.html">' > \
		$(DOCS)/index.html
	echo > $(DOCS)/.nojekyll

push-docs:
	cd $(DOCS) && \
		git add . && \
	    	git commit -m "Generated documentation" && \
		git push origin gh-pages:gh-pages
	git pull

nose: venv
	AMQP_URL=amqp:/// ./venv/bin/nosetests --cover-erase --with-coverage --cover-package puka --with-doctest
