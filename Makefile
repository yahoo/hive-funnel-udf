jar:
	mvn clean checkstyle:check package

test:
	mvn clean checkstyle:check test

code-coverage:
	mvn checkstyle:check cobertura:cobertura

clean:
	mvn clean
