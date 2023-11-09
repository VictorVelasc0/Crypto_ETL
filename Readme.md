# ETL Crypto Currency

El proyecto "ETL Crypto Currency" tiene como objetivo descargar datos desde la API CoinAPI. Esta API proporciona información sobre las tasas de cambio de criptomonedas en relación con monedas base, lo que permite realizar análisis y seguimiento de los valores de las criptomonedas en diferentes mercados y alertar en caso de un aumento respecto al histórico.

## Estructura del Proyecto

El proyecto está dividido en los siguientes apartados:

- `config`: Contiene archivos para la autenticación de la API y la configuración del Data Warehouse.
- `env_crypto`: Contiene el entorno virtual de Python necesario para ejecutar el script.
- `sql`: Contiene archivos SQL utilizados en el proyecto.
- `utils`: Contiene un conjunto de funciones que se utilizan para ejecutar el script.
- `logs`: Contiene logs de la ejecución del script de python.

## Uso de la API CoinAPI

La API CoinAPI utiliza el siguiente formato de URL base:

https://rest.coinapi.io/v1/exchangerate

Puedes realizar consultas específicas al agregar la denominación de la criptomoneda y la moneda base deseada al final de la URL. Por ejemplo:

- Para consultar la tasa de cambio de Bitcoin (BTC) con respecto al dólar estadounidense (USD):

https://rest.coinapi.io/v1/exchangerate/BTC/USD


- Para obtener tasas de cambio de todas las criptomonedas disponibles en relación con el dólar estadounidense (USD):

https://rest.coinapi.io/v1/exchangerate/USD


La API CoinAPI devuelve datos en formato JSON con los siguientes campos:

- `time`: La fecha y hora en la que se obtuvo la tasa de cambio.
- `asset_id_base`: La criptomoneda de origen.
- `asset_id_quote`: La moneda base con la que se expresa el valor de la criptomoneda.
- `rate`: La tasa de cambio actual.



La API CoinAPI devuelve datos en formato JSON con los siguientes campos:

- `time`: La fecha y hora en la que se obtuvo la tasa de cambio.
- `asset_id_base`: La criptomoneda de origen.
- `asset_id_quote`: La moneda base con la que se expresa el valor de la criptomoneda.
- `rate`: La tasa de cambio actual.

## Mapeo de Datos

Después de realizar una consulta a la API, los datos se mapean utilizando el siguiente convenio:

- `time` -> `created_at`: La fecha y hora en la que se obtuvo la tasa de cambio en formato Timestamp de Redshift.
- `asset_id_base` -> `Moneda`: La criptomoneda de origen.
- `asset_id_quote` -> `Base`: La moneda base con la que se expresa el valor de la criptomoneda.
- `rate` -> `Precio`: La tasa de cambio actual.

## Creación de la Tablas en Amazon Redshift

Se ha creado una tabla staging en Amazon Redshift con la siguiente estructura:

```sql
CREATE TABLE IF NOT EXISTS "data-engineer-database".dani_gt_10_coderhouse.stg_crypto
(
Moneda varchar(30) primary key distkey,
Base varchar(30),
Precio numeric,
created_at timestamp
)
sortkey(created_at); 
```
Esta tabla almacena los resultados dados por coinAPI en el momento de la consulta y se remplaza a cada ejecución

```sql
CREATE TABLE IF NOT EXISTS "data-engineer-database".dani_gt_10_coderhouse.tbl_crypto
(
Moneda varchar(30) primary key distkey,
Base varchar(30),
Precio numeric,
created_at timestamp,
updated_at timestamp,
executed_at timestamp
)
sortkey(created_at,updated_at,executed_at); 
```

## Optimización de Redshift
Se ha utilizado el optimizador de Amazon Redshift llamado **distkey** para la columna Moneda. Esto se hizo con la intención de optimizar la tabla para consultas utilizando el entorno MPP (Procesamiento de Datos en Paralelo Masivo) de Redshift. Cada slice dentro del nodo se divide por tipo de moneda de cripto, lo que permite una mejor optimización para consultas basadas en el tipo de moneda.

También se ha agregado otro **sortkey** en la columna created_at y updated_at, executed_at para organizar la información de cada nodo ordenada por fecha de creación seguido de la fehca de actualización y ejecución. Esto facilita la selección de valores utilizando la fecha como parámetro principal.

## Proceso Creación Tablas con Pandas
Después de la creación de las tablas y la optimización en Amazon Redshift, se utilizó la biblioteca Pandas en Python para insertar valores en la tabla. Este proceso ETL se completó con éxito para cargar y organizar los datos en el Data Warehouse en la base de datos **data-engineer-database** en el schema **dani_gt_10_coderhouse**.

## Documentación del Proceso ETL
Proceso de Carga SCD 1
Se ha implementado un proceso de carga SCD 1 (Slowly Changing Dimension 1) en el proyecto. Esto permite manejar los cambios en los datos y mantener un historial de las modificaciones. El siguiente SQL se utiliza para llevar a cabo este proceso:
```sql
MERGE INTO "data-engineer-database".dani_gt_10_coderhouse.tbl_crypto
USING "data-engineer-database".dani_gt_10_coderhouse.stg_crypto
ON tbl_crypto.Moneda=stg_crypto.Moneda AND tbl_crypto.created_at=stg_crypto.created_at
WHEN MATCHED THEN
    UPDATE SET 
    Precio = stg_crypto.Precio,
    created_at = stg_crypto.created_at,
    updated_at = GETDATE(),
    executed_at = GETDATE()
WHEN NOT MATCHED THEN
    INSERT (Moneda, Base, Precio, created_at, updated_at, executed_at)
    VALUES (stg_crypto.Moneda, stg_crypto.Base, stg_crypto.Precio, stg_crypto.created_at, GETDATE(), GETDATE())
```

Este SQL se utiliza para comparar los datos en la tabla **tbl_crypto** con los datos en la tabla de etapa **stg_crypto**. Si se encuentran la misma fecha de actualización de la consulta a la API created_at, se actualizan los registros existentes y se insertan nuevos registros en **tbl_crypto** con las fechas de modificación apropiadas.

## Antes de Comenzar

Antes de ejecutar el script, asegúrate de seguir estos pasos:

1. Descarga la carpeta `/config` con el archivo `config.ini`. Este archivo debe contener las credenciales de acceso a CoinAPI y Redshift. Coloca esta carpeta en el mismo nivel que los scripts `start.sh` y `build_script.sh`.
```
├── /config
│ ├── config.ini
├── /script
│ ├── script_cryptoAPI.py
├── /sql
│ ├── CREATE_STG_TBL_CRYPTO.sql
│ ├── CREATE_TBL_CRYPTO.sql
│ ├── MERGE_STG_TO_CRYPTO.sql
├── start.sh
├── build_script.sh
├── ...
```
## Iniciar el Proyecto
Para utilizar este proyecto, sigue estos pasos:
1. Clona el repositorio desde [URL del repositorio](https://github.com/VictorVelasc0/Crypto_ETL) o descarga el código fuente en tu máquina.

2. Asegúrate de tener Python instalado en tu sistema.

3. Ejecuta el siguiente comando para iniciar el entorno virtual de Python y descargar todas las dependencias necesarias:
Para utilizar este proyecto, sigue estos pasos:
```
sh build_script.sh
```
4. Ejecuta el siguiente comando para correr el script principal de Python una vez instaladas las dependencias necesarias:
```
sh start.sh
```
## Contribuciones
Las contribuciones a este proyecto son bienvenidas. Si deseas contribuir, asegúrate de crear un "fork" del repositorio y abrir una solicitud de extracción con tus cambios.

## Licencia
Este proyecto está licenciado bajo la Licencia MIT. Consulta el archivo LICENSE para obtener más información.


