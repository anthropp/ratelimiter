PROJECT  := anthropp
REGION   := us-west1
IMAGE    := $(REGION)-docker.pkg.dev/$(PROJECT)/ratelim/ratelim:latest
NS       := ratelimiter

.PHONY: test build image bootstrap redeploy url

test:
	go test ./...

build:
	go build ./...

# Build the container image remotely with Cloud Build (no local Docker needed).
image:
	gcloud builds submit --tag $(IMAGE) .

# One-time cluster setup; everything else is created by the loadgen itself.
bootstrap:
	kubectl apply -f deploy/

# Pick up a new image in the running system.
redeploy: image
	kubectl -n $(NS) rollout restart deployment/loadgen
	-kubectl -n $(NS) rollout restart deployment/coordinator deployment/ratelim-workers

url:
	@echo "http://$$(kubectl -n $(NS) get svc loadgen -o jsonpath='{.status.loadBalancer.ingress[0].ip}')"
