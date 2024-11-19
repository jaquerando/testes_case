# CLOUD RUN DEPLOYMENT

DAG - BRONZE 

CLOUD RUN DEPLOYMENT

# Artefact Registry
![image](https://github.com/user-attachments/assets/9683fb8e-5b85-4393-b9d3-937f7aab3206)

# Dag bronze service
![image](https://github.com/user-attachments/assets/63c8f096-75ea-4b7c-be52-23e1ebbaec69)

# Server message
![image](https://github.com/user-attachments/assets/3c7cc637-eb0c-4543-9892-1c586ee0417e)


# Dockerization and Deployment of Airflow DAGs: Summary of the Process
This is a detailed overview of how the Airflow DAGs were containerized, uploaded to Google Artifact Registry, and deployed on Google Cloud Run.

# 1. Preparation
A Docker Desktop environment was set up on the local machine, and its installation was verified using the command:

bash
Copy code
docker --version
The project was organized into directories for each DAG. 
The structure was as follows:

![image](https://github.com/user-attachments/assets/f84244f9-cf68-4a9e-9102-d2d0d44fd7d5)


This ensured that each DAG had its own folder containing the necessary Python script, requirements.txt, and Dockerfile.

# 2. Dockerfile Creation
A Dockerfile was created for each DAG. Below is an example for bronze:
dockerfile
Copy code
FROM python:3.9-slim

# Set the working directory inside the container
WORKDIR /app

# Copied the requirements file
COPY requirements.txt .

# Installed dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copied the DAG script
COPY . .

# Exposed the port for the container
EXPOSE 8080

# Command to start the server
CMD ["uvicorn", "bronze:app", "--host", "0.0.0.0", "--port", "8080"]
This structure was replicated for the silver and gold DAGs, replacing bronze with silver and gold in the respective files.

# 3. Docker Image Creation
The Docker images for each DAG were created. The terminal was navigated to each DAG's directory, and the following commands were executed:

bash
Copy code
cd airflow-to-cloudrun/bronze
docker build -t dag-bronze .
cd ../silver
docker build -t dag-silver .
cd ../gold
docker build -t dag-gold .
The created images were confirmed with:

bash
Copy code
docker images

# 4. Exporting Images
Each image was saved as a .tar file to facilitate transfer:
bash
Copy code
docker save dag-bronze > dag-bronze.tar
docker save dag-silver > dag-silver.tar
docker save dag-gold > dag-gold.tar

 # 5. Uploading to Cloud Shell
The .tar files were transferred to Google Cloud Shell using the following command:

bash
Copy code
gcloud compute scp dag-bronze.tar dag-silver.tar dag-gold.tar <username>@cloudshell:~ --zone=us-central1-a
Once transferred, the images were loaded into the Cloud Shell environment:

bash
Copy code
docker load < dag-bronze.tar
docker load < dag-silver.tar
docker load < dag-gold.tar

# 6. Artifact Registry Upload
An Artifact Registry repository was created using:

bash
Copy code
gcloud artifacts repositories create dag-repo \
  --repository-format=docker \
  --location=us-central1 \
  --description="Repository for DAG images"
The images were tagged for Artifact Registry:

bash
Copy code
docker tag dag-bronze us-central1-docker.pkg.dev/<project-id>/dag-repo/dag-bronze:latest
docker tag dag-silver us-central1-docker.pkg.dev/<project-id>/dag-repo/dag-silver:latest
docker tag dag-gold us-central1-docker.pkg.dev/<project-id>/dag-repo/dag-gold:latest
The tagged images were pushed to the registry:

bash
Copy code
docker push us-central1-docker.pkg.dev/<project-id>/dag-repo/dag-bronze:latest
docker push us-central1-docker.pkg.dev/<project-id>/dag-repo/dag-silver:latest
docker push us-central1-docker.pkg.dev/<project-id>/dag-repo/dag-gold:latest
The images were now accessible in the Google Artifact Registry.

# 7. Cloud Run Deployment
Each DAG was deployed to Google Cloud Run using the following commands:
bash
Copy code
gcloud run deploy dag-bronze \
  --image=us-central1-docker.pkg.dev/<project-id>/dag-repo/dag-bronze:latest \
  --region=us-central1 \
  --platform=managed \
  --allow-unauthenticated

gcloud run deploy dag-silver \
  --image=us-central1-docker.pkg.dev/<project-id>/dag-repo/dag-silver:latest \
  --region=us-central1 \
  --platform=managed \
  --allow-unauthenticated

gcloud run deploy dag-gold \
  --image=us-central1-docker.pkg.dev/<project-id>/dag-repo/dag-gold:latest \
  --region=us-central1 \
  --platform=managed \
  --allow-unauthenticated
  
# 8. Validation
After deployment, the services were tested. The logs were reviewed to ensure the services were running:

bash
Copy code
gcloud run services logs read dag-bronze
gcloud run services logs read dag-silver
gcloud run services logs read dag-gold
Each service was accessed via the URL provided by Cloud Run to confirm successful deployment.
