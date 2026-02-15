
import pyodbc
import psycopg2
from shapely import wkt
import psycopg2
import pyodbc
import os
from dotenv import load_dotenv


##SUBIDA DE ARCHIVO DE CREDENCIALES
load_dotenv(dotenv_path="credenciales_bd.env")
#%%
##CONEXIÓN A BD POSTGRESQL
pg_conn = psycopg2.connect(  

        host=os.getenv('PG_HOST'),
        port=os.getenv('PG_PORT'),
        dbname=os.getenv('PG_DB'),
        user=os.getenv('PG_USER'),
        password=os.getenv('PG_PWD')
    )
cursor_pg = pg_conn.cursor()


##CONNEXIÓN A BD SQL SERVER
host_sql = os.getenv('SQL_SERVER')
dbname_sql = os.getenv('SQL_DB')
user_sql = os.getenv('SQL_USER')
password_sql = os.getenv('SQL_PWD')

conn_sql = pyodbc.connect(
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={host_sql};"
    f"DATABASE={dbname_sql};"
    f"UID={user_sql};"
    f"PWD={password_sql};"
    f"Encrypt=no"
)
cursor_sql = conn_sql.cursor()
#%%
def conseguir_tablas(conn_sql,cursor_sql,pg_conn,cursor_pg):
    """
    Esta función guarda las tablas del modelo LADM tanto las que estan alojadas en postgreSQL como las que
    estan en SQL SERVER

    Args:
        conn_sql: Objeto de conexión a la base de datos SQL Server.
        cursor_sql: Cursor activo para ejecutar consultas en SQL Server.
        pg_conn: Objeto de conexión a la base de datos PostgreSQL.
        cursor_pg: Cursor activo para ejecutar consultas en PostgreSQL.

    Returns:
        tuple: Una tupla conteniendo dos objetos de tipo set (sql_tables, pg_tables).
    """

    
    query_sql = """
    SELECT TABLE_NAME 
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_TYPE = 'BASE TABLE'
    """
    cursor_sql.execute(query_sql)
    sql_tables = {row.TABLE_NAME for row in cursor_sql.fetchall()}

    
    query_pg = """
    SELECT tablename 
    FROM pg_tables 
    WHERE schemaname = 'public'
    """
    cursor_pg.execute(query_pg)
    pg_tables = {row[0] for row in cursor_pg.fetchall()}

    return sql_tables, pg_tables


def insertar_datos(sql_tables, pg_tables, log_file2,conn_sql,cursor_sql,pg_conn,cursor_pg):
    """
    Función que Compara las tablas de postgres y sql server, traspasa la información de sql server 
    a postgres de las tablas coincidentes, así mismo genera y escribe un archivo log con los 
    detalles de la transferencia

    Args:
        sql_tables (set): set de bvalores que contiene las tablas leidas en SQL SERVER 
        pg_tables (set): set de bvalores que contiene las tablas leidas en postgreSQL
        log_file2 : archivo log
        conn_sql: Objeto de conexión a la base de datos SQL Server.
        cursor_sql: Cursor activo para ejecutar consultas en SQL Server.
        pg_conn: Objeto de conexión a la base de datos PostgreSQL.
        cursor_pg: Cursor activo para ejecutar consultas en PostgreSQL.
    """
    log_file=log_file2
    matching_tables = {t.lower() for t in sql_tables}.intersection({t.lower() for t in pg_tables})

    if log_file2 == "Log_Remanante":
        orden_prioritario = [
            "ilc_predio",
            "ilc_derecho",
            "ilc_fuenteadministrativa",
            "ilc_caracteristicasunidadconstruccion","cr_unidadconstruccion",
            "ilc_datosadicionaleslevantamientocatastral",
            "cuc_tipologianoconvencional",
            "ilc_interesado","cuc_tipologiaconstruccion","cuc_calificacion_unidadconstruccion","cr_terreno"
            ,"t_ili2db_dataset","cr_puntolindero","cr_agrupacioninteresados", "col_miembros"
        ]
        tablas_prioritarias = [t for t in orden_prioritario if t in matching_tables]
        tablas_restantes = [t for t in matching_tables if t not in tablas_prioritarias]
        matching_tables = tablas_prioritarias + tablas_restantes

    with open(log_file2, "w", encoding="utf-8") as log:
        print(matching_tables)
        for table in matching_tables:
            print(table)
            try:
                cursor_pg.execute(f"SELECT COUNT(*) FROM public.{table}")
                initial_pg_count = cursor_pg.fetchone()[0]

                cursor_pg.execute(f"DELETE FROM public.{table} RETURNING *")
                deleted_rows = cursor_pg.rowcount
                pg_conn.commit()

                cursor_sql.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table}' AND TABLE_SCHEMA = 'dbo'")
                columns = [row.COLUMN_NAME for row in cursor_sql.fetchall()]

                geom_columns = [col for col in columns if 'geometria' in col.lower() or 'geometry' in col.lower() or  col.lower()=='localizacion']

                if geom_columns:
                    columns_str = ', '.join([f"[{col}]" for col in columns if col not in geom_columns])
                    geom_str = ', '.join([f"[{col}].STAsText() AS {col}_wkt" for col in geom_columns])
                    query = f"SELECT {columns_str}, {geom_str} FROM dbo.{table}"
                else:
                    query = f"SELECT * FROM dbo.{table}"

                cursor_sql.execute(query)
                rows = cursor_sql.fetchall()

                sql_count = len(rows)

                for row in rows:
                    row_data = list(row)
                    insert_columns = columns[:]
                    insert_values = ["%s"] * len(columns)
                    filtered_values = list(row_data)
                    geom_indices = [i for i, col in enumerate(columns) if col in geom_columns]
                    if geom_indices:
                        geom_col = insert_columns.pop(geom_indices[0])  
                        insert_columns.append(geom_col)  
                        geom_val = insert_values.pop(geom_indices[0]) 
                        insert_values.append("ST_Force3D(ST_GeomFromText(%s, 9377))")
                    insert_query = f"INSERT INTO public.{table} ({', '.join(insert_columns)}) VALUES ({', '.join(insert_values)})"

                    if table=="cr_terreno":
                        None

                    cursor_pg.execute(insert_query, filtered_values)
                pg_conn.commit()

                cursor_pg.execute(f"SELECT COUNT(*) FROM public.{table}")
                final_pg_count = cursor_pg.fetchone()[0]

                log.write(f"Tabla: {table} | SQL Server: {sql_count} registros | PostgreSQL Inicial: {initial_pg_count} | PostgreSQL Final: {final_pg_count}\n")

            except Exception as e:
                log.write(f"Error en la tabla {table}: {e}\n")
                pg_conn.rollback()


    print(f"LOG: {log_file}")



def insertar_tipos(sql_tables, pg_tables,conn_sql,cursor_sql,pg_conn,cursor_pg):
    """
    Función que inserta los valores de SQL SERVER a PostgreSQL de las tablas de dominio del 
    modelo LADM que terminan en tipo

    Args:
        sql_tables (set): set de bvalores que contiene las tablas leidas en SQL SERVER 
        pg_tables (set): set de bvalores que contiene las tablas leidas en postgreSQL
        conn_sql: Objeto de conexión a la base de datos SQL Server.
        cursor_sql: Cursor activo para ejecutar consultas en SQL Server.
        pg_conn: Objeto de conexión a la base de datos PostgreSQL.
        cursor_pg: Cursor activo para ejecutar consultas en PostgreSQL.
    """
    tables_to_include = {
        "extdireccion_clase_via_principal", "extdireccion_sector_ciudad", "extdireccion_sector_predio",
        "extdireccion_tipo_direccion", "extinteresado", "extredserviciosfisica","ilc_estructuranovedadnumeropredial_tipo_novedad"
    }
    
    filtered_tables = {t for t in sql_tables if (t in tables_to_include or t.endswith('tipo')) and not (t.startswith('vm_') or t.startswith('cuc_'))}

    insertar_datos(filtered_tables, pg_tables,"LogTipo",conn_sql,cursor_sql,pg_conn,cursor_pg)



def insertar_datos_cuc_vm(sql_tables, pg_tables, log_file3,conn_sql,cursor_sql,pg_conn,cursor_pg):
    """
    Función que actualiza en postgreSQL las llaves primarias de las tablas de dominio de los
    sumbodelos de valoración masiva y clasificación de construcción del modelo LADM.

    Args:
        sql_tables (set): set de bvalores que contiene las tablas leidas en SQL SERVER 
        pg_tables (set): set de bvalores que contiene las tablas leidas en postgreSQL
        log_file3 : archivo log
        conn_sql: Objeto de conexión a la base de datos SQL Server.
        cursor_sql: Cursor activo para ejecutar consultas en SQL Server.
        pg_conn: Objeto de conexión a la base de datos PostgreSQL.
        cursor_pg: Cursor activo para ejecutar consultas en PostgreSQL.
    """
    log_file = log_file3
    matching_tables = {t.lower() for t in sql_tables}.intersection({t.lower() for t in pg_tables})

    with open(log_file3, "w", encoding="utf-8") as log:
        for table in matching_tables:
            
                cursor_sql.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table}' AND TABLE_SCHEMA = 'dbo'")
                columns = [row.COLUMN_NAME for row in cursor_sql.fetchall()]

                if table in matching_tables:
                    cursor_sql.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table}'")
                    columns_sql = {col[0].lower(): col[0] for col in cursor_sql.fetchall()}

                    cursor_pg.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table.lower()}'")
                    columns_pg = {col[0].lower(): col[0] for col in cursor_pg.fetchall()}

                    id_column_sql = columns_sql.get("t_id", None)
                    id_column_pg = columns_pg.get("t_id", None)

                    if id_column_sql and id_column_pg:
                        cursor_sql.execute(f"SELECT {id_column_sql} FROM dbo.{table} ORDER BY {id_column_sql}")
                        ids_sql = [row[0] for row in cursor_sql.fetchall()]
                        cursor_pg.execute(f"SELECT ctid FROM public.{table} ORDER BY {id_column_pg}")
                        rows_pg = cursor_pg.fetchall()
                        if len(ids_sql) == len(rows_pg):
                            for idx, (ctid,) in enumerate(rows_pg):
                                cursor_pg.execute(f"UPDATE public.{table} SET {id_column_pg} = %s WHERE ctid = %s", (ids_sql[idx], ctid))

                            pg_conn.commit()
                            log.write(f"UPDATE de t_id realizado en {table} | Registros actualizados: {len(ids_sql)}\n")
                        else:
                            log.write(f"Error: Cantidad de registros en {table} no coincide entre SQL Server y PostgreSQL\n")
                    else:
                        log.write(f"Error: No se encontró la columna T_Id en {table}\n")
                

def insertar_tipos_cuc_vm(sql_tables, pg_tables,conn_sql,cursor_sql,pg_conn,cursor_pg):
    """
    Función que inicializa la función insertar_datos_cuc_vm, discriminando unicamente las tablas 
    de los submodelos de valoración masiva y de clasificación de construcción.

    Args:
        sql_tables (set): set de bvalores que contiene las tablas leidas en SQL SERVER 
        pg_tables (set): set de bvalores que contiene las tablas leidas en postgreSQL
        conn_sql: Objeto de conexión a la base de datos SQL Server.
        cursor_sql: Cursor activo para ejecutar consultas en SQL Server.
        pg_conn: Objeto de conexión a la base de datos PostgreSQL.
        cursor_pg: Cursor activo para ejecutar consultas en PostgreSQL.
    """

    tables_to_include = {}
    
    filtered_tables = {t for t in sql_tables if (t in tables_to_include or t.endswith('tipo')) and (t.startswith('vm_') or t.startswith('cuc_'))}
    insertar_datos_cuc_vm(filtered_tables, pg_tables,"LogCUC_VM",conn_sql,cursor_sql,pg_conn,cursor_pg)



def insertar_remanentes(sql_tables, pg_tables,conn_sql,cursor_sql,pg_conn,cursor_pg):
    """
    Función que inserta los datos de las demas tablas que no son de dominio, es decir la mayoría 
    de tablas que contienen los datos y dependen de las que contienen los dominios.

    Args:
        sql_tables (set): set de bvalores que contiene las tablas leidas en SQL SERVER 
        pg_tables (set): set de bvalores que contiene las tablas leidas en postgreSQL
        conn_sql: Objeto de conexión a la base de datos SQL Server.
        cursor_sql: Cursor activo para ejecutar consultas en SQL Server.
        pg_conn: Objeto de conexión a la base de datos PostgreSQL.
        cursor_pg: Cursor activo para ejecutar consultas en PostgreSQL.
    """
    tables_to_exclude = {
        "extdireccion_clase_via_principal", "extdireccion_sector_ciudad", "extdireccion_sector_predio",
        "extdireccion_tipo_direccion", 
        "extinteresado", "extredserviciosfisica",
          "ilc_estructuranovedadnumeropredial_tipo_novedad",
          "spatial_ref_sys","gm_curve2dlistvalue","gm_curve3dlistvalue","gm_surface2dlistvalue","gm_surface3dlistvalue"
    }
    
    filtered_tables = {t for t in sql_tables if not (t in tables_to_exclude or t.endswith('tipo'))}
    insertar_datos(filtered_tables, pg_tables, "Log_Remanante",conn_sql,cursor_sql,pg_conn,cursor_pg)

#%%
def main():
    """
    Función que inicializa todas las funciones anteriores y las ejecuta en el orden que requiere 
    la inserción de datos del modelo LADM aplicado a levantamiento catastral v 1.0.
    """
    sql_tables, pg_tables = conseguir_tablas(conn_sql,cursor_sql,pg_conn,cursor_pg)

    #compare_table_metadata(sql_tables, pg_tables,conn_sql,cursor_pg)

    insertar_tipos(sql_tables, pg_tables,conn_sql,cursor_sql,pg_conn,cursor_pg)
    insertar_tipos_cuc_vm(sql_tables, pg_tables,conn_sql,cursor_sql,pg_conn,cursor_pg)
    insertar_remanentes(sql_tables, pg_tables,conn_sql,cursor_sql,pg_conn,cursor_pg)
    
    #comparar_total_tablas(sql_tables, pg_tables)
    #print(sql_tables)

    print("---------------------------")
    cursor_sql.close()
    conn_sql.close()
    cursor_pg.close()
    pg_conn.close()
    #print(pg_tables)

if __name__ == "__main__":
    main()




