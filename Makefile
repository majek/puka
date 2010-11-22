CODEGEN_DIR=../rabbitmq-codegen
AMQP_JSON_SPEC=$(CODEGEN_DIR)/amqp-rabbitmq-0.9.1.json

PYTHON=python

all: vendor/rabbitmq-codegen/amqp_codegen.py puka/spec.py puka/spec_exceptions.py tests

vendor/rabbitmq-codegen/amqp_codegen.py:
	git submodule init
	git submodule update

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
	rm -f tests/.coverage
	rm -rf venv

distclean: clean
	rm -f puka/spec.py puka/spec_exceptions.py

.PHONY: tests prerequisites

test: tests
tests: puka/spec.py
	cd tests && AMQP_URL=amqp:/// PYTHONPATH=.. $(PYTHON) tests.py ../puka puka


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
	cd $(DOCS) && \
		git add . && \
	    	git commit -m "Generated documentation" && \
		git push origin gh-pages:gh-pages
	git pull

nose: venv
	AMQP_URL=amqp:/// ./venv/bin/nosetests --cover-erase --with-coverage --cover-package puka --with-doctest
