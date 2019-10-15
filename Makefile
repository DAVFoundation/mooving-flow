FORCE:

build-deploy: PROJECT=$(shell ./k8s/get_project.sh)
build-deploy: TIMESTAMP=$(shell date +%y%m%d-%H%M -u)
build-deploy: ENV=$(shell ./k8s/get_env.sh)
build-deploy: FORCE
	@echo Building for ${PROJECT} with timestamp $(TIMESTAMP)
	docker build . -f Dockerfile -t gcr.io/$(PROJECT)/flow:$(TIMESTAMP) --build-arg=VERSION=$(TIMESTAMP)
	docker tag gcr.io/$(PROJECT)/flow:$(TIMESTAMP) gcr.io/$(PROJECT)/flow:latest
	docker push gcr.io/$(PROJECT)/flow:$(TIMESTAMP)
	cd k8s && \
	until (ks show $(ENV) --ext-str IMAGE_VERSION=$(TIMESTAMP) \
		-c cassandra-backup \
		-c rider-payment \
		-c daily-stats \
		-c search-vehicles \
		-c end-ride \
		-c lock-vehicle \
		-c unlock-vehicle \
		-c cordon-vehicle \
		-c uncordon-vehicle \
		-c garage-vehicle \
		-c ungarage-vehicle \
		-c cordon-garage-vehicle \
		-c ungarage-uncordon-vehicle \
		-c dav-conversion-rate \
		-c update-dav-balance \
		-c token-txn \
		-c vehicle-updates \
		-o json > deploy.json); do sleep 1; done;
	kubectl apply -f k8s/deploy.json

local-deploy: TIMESTAMP=$(shell date +%y%m%d-%H%M -u)
local-deploy: FORCE
	@echo Building Local with timestamp $(TIMESTAMP)
	eval "$$(minikube docker-env)" &&\
		docker build . -f Dockerfile -t gcr.io/mooving-development/flow:$(TIMESTAMP) --build-arg=VERSION=$(TIMESTAMP)

	cd k8s && ks show local -o json --ext-str IMAGE_VERSION=$(TIMESTAMP) \
		-c garage-vehicle \
		-c ungarage-vehicle \
		-c vehicle-updates \
		> dist/deploy.json

	kubectl apply -f k8s/dist/deploy.json
