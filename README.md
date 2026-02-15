# LADM-etl-sqlserver-to-postgres

<img width="885" height="270" alt="image" src="https://github.com/user-attachments/assets/92f5cee6-6f71-46c6-972e-9dbd4bb31d14" />

📋 Descripción
Este proyecto resuelve la interoperabilidad entre bases de datos institucionales. Desarrollé un algoritmo ETL en Python para migrar información catastral desde SQL Server hacia un modelo optimizado en PostgreSQL, garantizando la integridad del estándar LADM-COL para la generación de archivos XTF.

🛠️ Tecnologías
Lenguaje: Python 3.x

Librerías: psycopg2, pyodbc, python-dotenv

Estándar Geográfico: LADM-COL / PostGIS

🏗️ Arquitectura del Proceso
(Aquí insertas la imagen del diagrama que hiciste, el de las flechas y logos).

🚀 Instalación y Uso
Clonar el repositorio.

Crear un archivo .env basado en .env.example.

Ejecutar: python SQL_TO_POSTGRES.py

⚙️ Requisitos Previos (Pre-requisitos)
Para que el algoritmo ETL funcione correctamente, es indispensable cumplir con la siguiente arquitectura de destino:

Esquema LADM-COL: La base de datos en PostgreSQL debe estar previamente creada y vacía.

Modelo Aplicado: Se debe haber cargado el Modelo Aplicado a Levantamiento Catastral v_1.0.

Herramienta de Estructuración: El esquema debe ser generado obligatoriamente mediante iliSuite, asegurando que todas las tablas, dominios y restricciones espaciales estén alineados con el estándar nacional antes de iniciar la migración desde SQL Server.

Instalar archivo de librerias prerequisito. 

pip install -r requirements.txt

[!IMPORTANT]
El script no crea tablas ni esquemas; su función es el mapeo, transformación y carga de datos entre un origen en SQL Server y un destino ya estructurado bajo la normativa LADM-COL.
