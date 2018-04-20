
test:
	 sbt test

run-mist:
	 ./run-mist-docker.sh

package:
	 sbt package

deploy-csv:
	 ./deploy-csv.sh
