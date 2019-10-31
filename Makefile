PROJECT=<<GCP_PROJECT_ID>>

FORCE:

deploy: TIMESTAMP=$(shell date +%y%m%d-%H%M -u)
deploy: FORCE
	@echo Building Local with timestamp $(TIMESTAMP)
	eval "$$(minikube docker-env)" &&\
		docker build . -f Dockerfile -t gcr.io/$(PROJECT)/flow:$(TIMESTAMP) --build-arg=VERSION=$(TIMESTAMP)

	cd k8s && ks show local -o json --ext-str IMAGE_VERSION=$(TIMESTAMP) \
		-c garage-vehicle \
		-c ungarage-vehicle \
		-c vehicle-updates \
		> dist/deploy.json

	kubectl apply -f k8s/dist/deploy.json
