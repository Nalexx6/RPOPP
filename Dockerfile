# Use an official Spark image as the base image
FROM apache/spark:3.4.0

# Set the working directory in the container
WORKDIR /opt/spark

# Copy the Python scripts from your local machine into the container
COPY ./etl/airflow/dags/ ./dags

# Optionally, you can install any additional dependencies using pip
RUN pip install -r ./dags/requirements.txt
