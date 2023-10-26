#!/bin/bash

# Desactiva el entorno virtual si está activado
if [ -n "$VIRTUAL_ENV" ]; then
    deactivate
    echo "Entorno virtual desactivado."
fi

# Borra el directorio del entorno virtual si existe
if [ -d "env_crypto" ]; then
    rm -r env_crypto
    echo "Entorno virtual 'env_crypto' eliminado."
fi

# Crear un entorno virtual llamado "env_crypto"
echo "Creando entorno virtual env_crypto"
python3 -m venv env_crypto

# Activar el entorno virtual
echo "Activando entorno env_crypto"
source env_crypto/bin/activate

# Instalar las dependencias
echo "Instalando dependencias"
pip install requests
pip install pandas
pip install configparser
pip install urllib3==1.26.6
pip install sqlalchemy==1.4.36 psycopg2-binary
pip install "redshift_connector[full]" sqlalchemy-redshift


# Desactivar el entorno virtual
echo "Dependencias instaladas :) "
echo "Para ejecutar script ingrese en la termina ./start.sh"
deactivate
