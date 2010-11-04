CODEGEN_DIR=../rabbitmq-codegen
AMQP_JSON_SPEC=$(CODEGEN_DIR)/amqp-rabbitmq-0.9.1.json

PYTHON=python

all: puka/spec.py tests

$(AMQP_JSON_SPEC):
	@echo "You need '$(CODEGEN_DIR)' package."
	@echo "Try one of the following:"
	@echo "  git clone git://github.com/rabbitmq/rabbitmq-codegen.git"
	@echo "  hg clone http://hg.rabbitmq.com/rabbitmq-codegen"
	@exit 1

puka/spec.py: codegen.py $(CODEGEN_DIR)/amqp_codegen.py \
		$(AMQP_JSON_SPEC) amqp-accepted-by-update.json
	$(PYTHON) codegen.py framing $(AMQP_JSON_SPEC) puka/spec.py

clean:
	find . -name \*pyc|xargs --no-run-if-empty rm
	rm -f tests/.coverage


.PHONY: tests prerequisites

test: tests
tests: puka/spec.py
	cd tests && AMQP_URL=amqp:/// PYTHONPATH=.. $(PYTHON) tests.py ../puka puka
