

# FinAnalytics Data Product

The **FinAnalytics Data Product** is an automated, production-ready ETL solution designed to reliably collect, transform, and load financial market data from the [Finnhub API](https://finnhub.io) into your data warehouse for real-time analytics. This pipeline leverages modern technologies to ensure scalability, maintainability, and high data quality.

---

## Table of Contents

- [Overview](#overview)
- [Repository Structure](#repository-structure)
- [Architecture & Technologies](#architecture--technologies)
- [Installation & Setup](#installation--setup)
  - [Local Development](#local-development)
  - [Production Deployment](#production-deployment)
- [Usage](#usage)
- [Development Workflow](#development-workflow)
- [Testing & Code Quality](#testing--code-quality)
- [Contributing](#contributing)
- [Future Enhancements](#future-enhancements)
- [License](#license)
- [Contact](#contact)

---

## Overview

The **FinAnalytics Data Product** automates the ETL (Extract, Transform, Load) process for financial data:

- **Data Ingestion:** Retrieves stock market data from Finnhub on a scheduled basis.
- **Data Transformation:** Cleans, normalizes, and enriches raw data using Python and PySpark.
- **Data Loading:** Moves processed data into a data warehouse, making it available for analytics and dashboarding.

This solution is built to meet the demands of production environments, using robust orchestration, containerization, and cloud infrastructure practices.

---

## Repository Structure

```
FinAnalytics-Product/
└── apps/
    ├── prod_setup.sh                       # Production Kubernetes orchestrator platform setup
└── finnhub-batch-stock-pipeline/
    ├── finnhub-batch-stock-pipeline/       # Dagster pipeline definitions
    ├── README.md                           # Project documentation (this file)
    ├── setup.py                            # Python package setup & dependency management
    ├── finnhub_batch_stock_pipeline_tests/ # Unit tests (Pytest)
└── terraform/                              # Infrastructure provisioning scripts
    ├── .terraform/
    ├── config.tf
    ├── dgstr_k8s_helm_chart.tf
    ├── providers.tf
    ├── secrets.tf
    ├── values.yaml
    ├── variables.tf
    ├── volumes.tf
├── Dockerfile                              # Containerization instructions
├── env.txt                                 # Example environment variables
├── local_requirements.txt                  # Local project dependencies
├── requirements.txt                        # Production project dependencies
```

---

## Architecture & Technologies

This project employs a modern, modular architecture:

- **Orchestration:** [Dagster](https://dagster.io/) – Manages and schedules ETL processes as modular assets.
- **Data Processing:** Python & [PySpark](https://spark.apache.org/docs/latest/api/python/) – Handles large-scale data transformations.
- **Containerization:** [Docker](https://www.docker.com/) – Ensures consistency across environments.
- **Deployment:** [Kubernetes](https://kubernetes.io/) – Orchestrates containerized deployments at scale.
- **Cloud Infrastructure:** [AWS](https://aws.amazon.com/) – Provides scalable compute and storage resources.
- **Infrastructure as Code:** [Terraform](https://www.terraform.io/) – Automates cloud resource provisioning.
- **Data Quality:** [Great Expectations](https://greatexpectations.io/) – Validates data integrity at each stage.
- **Testing & Linting:** [Pytest](https://docs.pytest.org/), [Flake8](https://flake8.pycqa.org/), and [Black](https://black.readthedocs.io/)

---

## Installation & Setup

### Local Development

1. **Clone the Repository**

   ```bash
   git clone https://github.com/WillowyBoat2388/finanalytics-product.git
   cd finanalytics-product/finnhub-batch-stock-pipeline
   ```

2. **Install Dependencies** Install the package in editable mode for live development:

   ```bash
   pip install -e ".[dev]"
   pip install local_requirements.txt
   ```

3. **Start the Dagster Development Server** Launch the server to run and monitor your pipeline:

   ```bash
   cd finnhub-batch-stock-pipeline
   dagster dev
   ```

   Access the Dagster UI at [http://localhost:3000](http://localhost:3000) to review pipeline runs, manage schedules, and view logs.

4. **Run Unit Tests** Verify functionality with:

   ```bash
   cd finnhub-batch-stock-pipeline
   pytest finnhub_batch_stock_pipeline_tests
   ```

### Production Deployment

1. **Build the Docker image**

   ```bash
   docker build -t finnhub-batch-stock-pipeline:latest .
   ```

2. **Push the Docker Image** Replace `<registry-url>` and `<your-image>` with your container registry details:

   ```bash
   docker tag finnhub-batch-stock-pipeline:latest <registry-url>/<your-image>:latest
   docker push <registry-url>/<your-image>:latest
   ```

3. **Deploy with Kubernetes** Apply the deployment configuration:

   ```bash
   kubectl apply -f deployment.yaml
   ```

4. **Provision AWS Infrastructure with Terraform** Navigate to the Terraform configuration directory and execute:

   ```bash
   cd terraform
   terraform init
   terraform apply -auto-approve
   ```

Potentially, these would have been the steps which would have needed to be taken to get up and running. To simplify setup however, the entire production system can be setup and running by just following these steps.

1. **Run the Setup bash script**
    ```
    bash apps/prod_setup.sh
    ```

**Note**: This can **only** be done on a Linux machine

---

## Usage

- **Define Data Assets:** Modify the assets within the `finnhub-batch-stock-pipeline/finnhub-batch-stock-pipeline/assets` folder to create or update Dagster assets.
- **Run the Pipeline:** With `dagster dev` running, the pipeline’s scheduler and sensors will automatically trigger ETL jobs.
- **Monitor Logs:** Check pipeline logs with:
  ```bash
  tail -f logs/dagster.log
  ```

---

## Development Workflow

- **Live Development:** Use `dagster dev` for real-time updates.
- **Add Dependencies:** Update `setup.py` for any new libraries.
- **Code Quality & Testing:** Ensure your changes meet standards:
  ```bash
  flake8
  black .
  pytest finnhub_batch_stock_pipeline_tests
  ```

---

## Testing & Code Quality

- **Unit Testing:** Conducted with Pytest to verify each component.
- **Linting & Formatting:** Enforced via Flake8 and Black to maintain a consistent code style.
- **Data Quality Assurance:** Integrated with Great Expectations to validate data at every stage.

---

## Contributing

Contributions are welcome! To contribute:

1. **Fork the Repository:** Create your own copy.
2. **Create a Feature Branch:** Make your changes on a new branch.
3. **Implement & Test:** Write your code and ensure all tests pass.
4. **Submit a Pull Request:** Provide a clear description of your changes and the problem they solve.

---

## Future Enhancements

- **Real-Time Data Streaming:** Expand to support real-time data ingestion.
- **Enhanced Monitoring:** Integrate tools like Prometheus and Grafana for in-depth monitoring.
- **Auto-Scaling:** Leverage Kubernetes auto-scaling for dynamic resource management.
- **Dashboard Integration:** Enhance visualizations by integrating with BI tools.
- **Security Improvements:** Implement additional security measures for API and data encryption.

---

## License

This project is licensed under the **MIT License** – see the [LICENSE](LICENSE) file for details.

---

## Contact

For questions or further support, please reach out to **Judge Fikayo** at **[**[**onidajo99@gmail.com**](mailto\:onidajo99@gmail.com)**]**.



This is a [Dagster](https://dagster.io/) project scaffolded with [`dagster project scaffold`](https://docs.dagster.io/getting-started/create-new-project).

### Description
    This repository contains a data engineering pipeline, that is used to fetch data from FinnHub API for a batch of stocks before it is then transformed and loaded into a Data Warehouse. After the ETL process is completed, the transformed data is then to be used to create insights in the form of a dashboard.

### Infrastructure
#### Tools & Services:
- Dagster
- Kubernetes
- Python
- AWS
- Pyspark

## Getting started
Open http://localhost:3000 with your browser to see the project.

### Schedules and sensors

If you want to enable Dagster [Schedules](https://docs.dagster.io/concepts/partitions-schedules-sensors/schedules) or [Sensors](https://docs.dagster.io/concepts/partitions-schedules-sensors/sensors) for your jobs, the [Dagster Daemon](https://docs.dagster.io/deployment/dagster-daemon) process must be running.
Once your Dagster Daemon is running, you can start turning on schedules and sensors for your jobs.
