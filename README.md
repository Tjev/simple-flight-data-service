# FAA Data service

### Description
Implementation of a simple data provisioning service with REST API endpoints. 

## Usage

A build of the docker image from the provided Dockerfile is required first.
To build the image, make sure youre in the same directory as the Dockerfile and use the following:
```bash
docker build --rm -t sdf_service .
```


Then simply run the `run_app.sh`. 

The service API will be then available at `http://localhost:8000/`.

## Docs

The (Swagger) API docs are available at `http://localhost:8000/docs`.
