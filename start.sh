#!/bin/bash

# Activar el entorno virtual
source env_crypto/bin/activate

# Opcional: Puedes agregar comandos adicionales aquí si es necesario
# Ejemplo: cd /ruta/a/tu/proyecto

echo "Entorno virtual 'env_crypto' ha sido activado."

echo "Ejecutando archivo script"
python3 ./script/script_cryptoAPI.py

#Stop virtual env
deactivate
echo "Entorno virtual 'env_crypto' ha sido desactivado."