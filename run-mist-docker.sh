#!/bin/bash

docker run -p 2004:2004 \
	 -v /var/run/docker.sock:/var/run/docker.sock \
	 hydrosphere/mist:1.0.0-RC14-2.2.0
