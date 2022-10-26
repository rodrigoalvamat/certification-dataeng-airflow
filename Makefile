.PHONY : all clean deploy destroy init

all:
	deploy

clean:
	find ./dags -name '*.py[co]' -exec rm {} \;
	find ./dags -name '__pycache__' -exec rm -rf ||: {}\;
	find ./plugins -name '*.py[co]' -exec rm {} \;
	find ./plugins -name '__pycache__' -exec rm -rf ||: {}\;

deploy: clean
	terraform -chdir='./terraform' apply -var-file='secret.tfvars'

destroy:
	terraform -chdir='./terraform' destroy -var-file='secret.tfvars'

init:
	terraform -chdir='./terraform' init

install:
	pipenv install