test:
	make lint
	mocha --recursive --check-leaks --timeout 10000

lint:
	./node_modules/.bin/jshint . --verbose

.PHONY : test lint