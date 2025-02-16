"""
🍰 Una pastelería quiere generar un reporte diario de pedidos y producción, guardarlo como Excel y subirlo a Google Cloud Storage 🍰
📌 Ejemplo de cómo funciona en la pastelería 
Paso 1: Inicio 📅 El robot comienza su trabajo:
"Hoy es 16 de febrero, vamos a ver si hay pedidos para hoy". 

Paso 2: Verificar si hay pedidos ✅ El robot revisa la base de datos:
"Sí, tenemos pedidos de la Sucursal Centro y la Sucursal Norte". 

Paso 3: Generar el reporte 📊 El robot crea un archivo Excel con dos hojas: 
• Pedidos del Día • Cliente: Juan Pérez 
• Producto: Pastel de chocolate 
• Cantidad: 2 • Estado: Pagado   
• Producción Programada 
• Producto: Pastel de chocolate 
• Cantidad total: 10 
• Ingredientes necesarios: Harina, azúcar, cacao    

Paso 4: Subir el archivo a la nube ☁️ 
El robot guarda el Excel en la nube:
"Subí el reporte a Google Cloud Storage, ya está disponible para que lo revisen". 

Paso 5: Borrar el archivo temporal 🗑️ El robot borra el archivo de la computadora para ahorrar espacio.
"""
import os
import pandas as pd
import rutinas_pasteleria  # Módulo de funciones personalizadas

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import XCom, Variable
from airflow.utils.db import provide_session
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.bash import BashOperator

from google.cloud import bigquery, storage
from google.cloud.exceptions import BadRequest

from openpyxl import load_workbook

# 🔹 Configuración del DAG
DAG_ID = 'reporte_pedidos_pasteleria'
PROJECT_ID = Variable.get('project_id')
DATASET_ID = 'Pasteleria_DWH'
OUTPUT_BUCKET = Variable.get('bucket_reportes_pasteleria')
OUTPUT_FOLDER = 'Pedidos'
LAYOUT_NAME = 'Reporte_Pedidos_Diario'
DEPENDENCY_NAME = 'Pedidos_Diarios'

# 🔹 Encabezados del archivo Excel
FILE_HEADER = [
    'Sucursal', 
    'Cliente', 
    'Fecha Pedido', 
    'Producto', 
    'Cantidad', 
    'Estado'
]

# 🔹 Tablas de BigQuery
TABLES = {
    'Pedidos': ["Pedidos_Dia"],
    'Produccion': ["Produccion_Programada"]
}

default_args = {
    'owner': 'Pasteleria Central 🍰',
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

# 🔹 Función para limpiar XCom (datos temporales de Airflow)
@provide_session
def cleanup_xcom(session=None):
    session.query(XCom).filter(XCom.dag_id == DAG_ID).delete()

# 🔹 Obtener la fecha actual
def set_current_date(**kwargs):
    current_date = datetime.now().strftime('%Y-%m-%d')
    kwargs['ti'].xcom_push(key='current_date', value=current_date)

# 🔹 Verificar si debe ejecutarse el DAG
def verificar_pedidos(**kwargs):
    try:
        date = kwargs['ti'].xcom_pull(key='current_date')
        if rutinas_pasteleria.verificar_pedidos_programados(DEPENDENCY_NAME, date):
            return 'generar_reporte_pedidos'
        return 'fin'
    except Exception as e:
        print(f"Error: {e}")
        raise e

# 🔹 Generar reporte en Excel
def generar_reporte_pedidos(**kwargs):
    try:
        working_directory = Variable.get("airflow_persistent_directory")

        if not os.path.exists(working_directory):
            os.makedirs(working_directory)

        kwargs['ti'].xcom_push(key="file_path", value=working_directory)

        with pd.ExcelWriter(f"{working_directory}/{LAYOUT_NAME}.xlsx", engine="openpyxl") as writer:
            for index, SHEET_NAME in enumerate(TABLES):
                query = rutinas_pasteleria.generar_query_pedidos(DATASET_ID, TABLES[SHEET_NAME])
                bq_client = bigquery.Client()
                df = bq_client.query(query).result().to_dataframe()

                if df.empty:
                    raise ValueError("No hay pedidos en la tabla")

                df = df[df.iloc[:, 0].notna()]
                df.to_excel(writer, sheet_name=SHEET_NAME, index=False)
    except Exception as e:
        print(f"Error: {e}")
        raise e

# 🔹 Subir archivo a Google Cloud Storage
def subir_reporte_a_gcs(**kwargs):
    file_path = kwargs['ti'].xcom_pull(key='file_path')

    gcs_client = storage.Client()
    bucket = gcs_client.get_bucket(OUTPUT_BUCKET)
    blob = bucket.blob(f"{OUTPUT_FOLDER}/{LAYOUT_NAME}.xlsx")
    blob.upload_from_filename(f"{file_path}/{LAYOUT_NAME}.xlsx")

# 🔹 Definir DAG
with DAG(
    DAG_ID,
    schedule_interval=None,
    start_date=datetime(2024, 10, 14),
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=['Reportes Pasteleria']
) as dag:

    inicio = DummyOperator(task_id="inicio")
    obtener_fecha = PythonOperator(task_id="obtener_fecha", python_callable=set_current_date)
    verificar = BranchPythonOperator(task_id="verificar_pedidos", python_callable=verificar_pedidos)
    generar_reporte = PythonOperator(task_id="generar_reporte_pedidos", python_callable=generar_reporte_pedidos)
    subir_a_gcs = PythonOperator(task_id="subir_reporte_a_gcs", python_callable=subir_reporte_a_gcs)
    eliminar_archivo_local = BashOperator(task_id="eliminar_archivo_local", bash_command="rm '{{ task_instance.xcom_pull(key='file_path') }}/" + LAYOUT_NAME + ".xlsx'")
    fin = DummyOperator(task_id="fin", trigger_rule="none_failed")

    # 🔹 Definir dependencias
    inicio >> obtener_fecha >> verificar >> [generar_reporte >> subir_a_gcs >> eliminar_archivo_local, fin]
