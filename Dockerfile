# Use an official Spark image as the base image
FROM nalexx06/nalexx6-spark:3.4.0

# Set the working directory in the container
WORKDIR /opt/spark

# Copy the Python scripts from your local machine into the container
COPY ./etl/airflow/dags/ ./dags