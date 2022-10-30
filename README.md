# Data Pipelines with Airflow & AWS Redshift

---

> Author: Rodrigo de Alvarenga Mattos 

## Introduction

The intent of this project is to build an automated ETL pipeline using Apache Airflow. The data is a numerous set of small JSON files, stored in AWS S3, that must be loaded into AWS Redshift according to the star schema described in the Table Schema section. The pipeline was built from reusable tasks and the data quality check can be easily extended to connect with any database.

## Auto-generate API Documentation

The [Sphinx](https://www.sphinx-doc.org) documentation generator was used to build the [HTML docs](http://htmlpreview.github.io/?https://github.com/rodrigoalvamat/certification-dataeng-airflow/blob/main/docs/build/html/index.html) from the source code ```DOCSTRIGS```.

## Project Dependencies

- [Apache Airflow 2.4.1](https://airflow.apache.org/docs/apache-airflow/2.4.1/start.html)

- [Pipenv 2022.10.25](https://pipenv.pypa.io/en/latest/)

- [Python 3.10](https://www.python.org)

- [Terraform 1.2.9](https://www.terraform.io)

- [Sphinx 5.3.0](https://www.sphinx-doc.org)
1. The Pipenv package manager was used to install and manage dependencies.

```bash
# install pipenv for dependency management
pip install pipenv

# install project dependencies from Pipfile
pipenv install
```

2. You can also use pip to install dependencies from the requirements.txt file.

```bash
pip install -r requirements.txt
```

3. Install Apache Airflow using docker compose:

```bash
make docker
```

## AWS Services and Resources

This is the list of services that have been provisioned in the AWS cloud:

| Service      | Resources                    | Description                              |
| ------------ | ---------------------------- | ---------------------------------------- |
| **IAM**      | Policy                       | Redshift policy.                         |
| **IAM**      | Role and User                | Redshift role and user.                  |
| **S3**       | Data Lake Bucket             | Storage data source.                     |
| **VPC**      | Subnet Group and VPN Gateway | Redshift virtual networking environment. |
| **VPC**      | Security Group               | Redshift inbound and outbound traffic.   |
| **Redshift** | EC2 instances.               | Customized cloud data warehouse.         |

## Terraform Infrastructure as a Code

We used Terraform to automate infrastructure provisioning, including servers, network, permissions, and security. Please follow the instructions below before running Terraform commands:

1. Check if you have terraform installed or follow the [instructions in the website](https://learn.hashicorp.com/tutorials/terraform/install-cli):

```bash
terraform version
```

2. Make sure you have the [AWS Command Line Interface](https://aws.amazon.com/cli) installed, the user is logged in and the default region is set:

```bash
# check the current user
aws iam get-user

# the default region should be set
aws configure get region
```

3. Initialize the Terraform working directory containing configuration files:

```bash
make init
```

4. Apply Terraform configuration to provision AWS services and resources:

```bash
make apply
```

## Apache Airflow Webserver Configuration

Before running the ETL pipeline, you must login into the Airflow UI to setup the AWS credentials and the Redshift connection.