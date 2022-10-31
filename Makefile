.PHONY : all apply clean destroy doc docker init

USERID := $(shell id -u)

all:
	apply

clean:
	find ./dags -name '*.py[co]' -exec rm {} \;
	find ./dags -name '__pycache__' -exec rm -rf ||: {}\;
	find ./plugins -name '*.py[co]' -exec rm {} \;
	find ./plugins -name '__pycache__' -exec rm -rf ||: {}\;

apply: clean
	terraform -chdir='./terraform' apply -var-file='secret.tfvars' -auto-approve

destroy:
	terraform -chdir='./terraform' destroy -var-file='secret.tfvars' -auto-approve

doc:
	sphinx-build -M html ./docs/source ./docs/build

docker:
	mkdir -p ./logs
	echo -e "AIRFLOW_UID=$(USERID)" > .env
	docker-compose up airflow-init
	docker-compose up -d

init:
	terraform -chdir='./terraform' init