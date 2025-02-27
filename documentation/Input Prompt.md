# Input: Composer 1 Airflow 1.10.15 Code Migration to Cloud Composer 2 Airflow 2

## Objective

Convert the provided composer 1 airflow 1 codebase to:

1. Composer 2 Airflow 2.X(latest)

   - Create a CICD for the DAGs to be deployed to GCP GCS bucket - dag_bucket. Make sure DEV requires a peer review and QA requires a peer and QA review and PROD requires CAB approval, Architect approval and Stakeholder approval in the pull request.

## Requirements

- Maintain existing functionality and business logic

- Add code comments

- Create test cases for the airflow DAGs that can be run on local Airflow set up

- Update dependency versions as needed

- Retain original naming conventions

- Create a CICD for the DAGs to be deployed to GCP GCS bucket - dag_bucket. Make sure DEV requires a peer review and QA requires a peer and QA review and PROD requires CAB approval, Architect approval and Stakeholder approval in the pull request.

## Scope

- Repository code only

- Direct version migration without additional feature changes

- Like-for-like conversion where possible

Note: Please preserve original comments and documentation structure.