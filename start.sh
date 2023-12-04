#!/bin/bash

# Ejecutar contenedor y levantarlo
docker compose up -d
docker compose ps
echo "The container is running in the background"
echo "For display the web server http://localhost:8081/"