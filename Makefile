test:
	@make lint
	@./node_modules/.bin/mocha --recursive --check-leaks --timeout 10000

lint:
	@./node_modules/.bin/jshint . --verbose

test-codecov.io:
	@NODE_ENV=test ./node_modules/.bin/istanbul cover \
	./node_modules/mocha/bin/_mocha --report lcovonly -- -R spec && \
		cat ./coverage/lcov.info | ./node_modules/codecov.io/bin/codecov.io.js --verbose

.PHONY : test lint
