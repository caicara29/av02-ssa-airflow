"""
AV-02 - SSA - Airflow
Pipeline de processamento do dataset de streaming musical
"""
import pendulum
import pandas as pd

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import get_current_context
from airflow.utils.trigger_rule import TriggerRule


# Caminhos dos arquivos dentro do container do Airflow
DATA_DIR = "/opt/airflow/data"
ARQ_ORIGEM = f"{DATA_DIR}/dados-stream.csv"
ARQ_ENTRADA = f"{DATA_DIR}/entrada.csv"
ARQ_TASK2 = f"{DATA_DIR}/task2.csv"
ARQ_TASK3 = f"{DATA_DIR}/task3.csv"
ARQ_TASK4 = f"{DATA_DIR}/task4.csv"
ARQ_MEDIA = f"{DATA_DIR}/media_avaliacao.csv"
ARQ_TOTAL_ARTISTA = f"{DATA_DIR}/total_artista.csv"


# ----------------------------- Funções Python -----------------------------

def tratar_datas():
    """TASK-2: padroniza todas as datas no formato dd/mm/aaaa."""
    df = pd.read_csv(ARQ_ENTRADA, sep=";", encoding="utf-8-sig")

    # pd.to_datetime com format="mixed" reconhece os dois formatos presentes
    # (dd/mm/aaaa e aaaa-mm-dd) e converte para datetime
    df["data_execucao"] = pd.to_datetime(
        df["data_execucao"], format="mixed", dayfirst=True
    )
    # Reescreve sempre no padrão brasileiro
    df["data_execucao"] = df["data_execucao"].dt.strftime("%d/%m/%Y")

    df.to_csv(ARQ_TASK2, sep=";", index=False)
    print(f"task2.csv gerado com {len(df)} linhas")


def remover_musicas_vazias():
    """TASK-3: remove linhas com nome_musica vazio e empurra o total
    descartado para a próxima task via XCom."""
    df = pd.read_csv(ARQ_TASK2, sep=";")
    total_inicial = len(df)

    # Considera vazio tanto NaN quanto string em branco
    mask_validas = df["nome_musica"].notna() & (
        df["nome_musica"].astype(str).str.strip() != ""
    )
    df_limpo = df[mask_validas].copy()

    total_descartado = total_inicial - len(df_limpo)

    df_limpo.to_csv(ARQ_TASK3, sep=";", index=False)
    print(f"Total descartado: {total_descartado}")

    # Manda o número de descartados pro XCom (consumido pela TASK-4)
    get_current_context()["ti"].xcom_push(
        key="total_descartado", value=total_descartado
    )


def enriquecer_com_genero():
    """TASK-6: cria a coluna nome_genero usando o resultado da TASK-5."""
    ti = get_current_context()["ti"]

    # Resultado da TASK-5 vem como lista de tuplas: [('001','POP'), ...]
    generos_raw = ti.xcom_pull(task_ids="task_5_consulta_generos")
    df_gen = pd.DataFrame(generos_raw, columns=["id_genero", "nome_genero"])

    df = pd.read_csv(ARQ_TASK3, sep=";", dtype={"id_genero": str})

    # Garante que o id_genero esteja como string com zeros à esquerda
    df["id_genero"] = df["id_genero"].astype(str).str.zfill(3)
    df_gen["id_genero"] = df_gen["id_genero"].astype(str).str.zfill(3)

    df_enriquecido = df.merge(df_gen, on="id_genero", how="left")

    df_enriquecido.to_csv(ARQ_TASK4, sep=";", index=False)
    print(f"task4.csv gerado com {len(df_enriquecido)} linhas")


def media_avaliacao_por_musica():
    """TASK-7: média de nota por música."""
    df = pd.read_csv(ARQ_TASK4, sep=";")
    media = (
        df.groupby("nome_musica")["nota"]
        .mean()
        .round(2)
        .reset_index()
        .rename(columns={"nota": "media_nota"})
        .sort_values("media_nota", ascending=False)
    )
    media.to_csv(ARQ_MEDIA, sep=";", index=False)
    print(media)


def total_musicas_por_artista():
    """TASK-8: total de músicas ouvidas por artista."""
    df = pd.read_csv(ARQ_TASK4, sep=";")
    total = (
        df.groupby("nome_artista")
        .size()
        .reset_index(name="total_musicas")
        .sort_values("total_musicas", ascending=False)
    )
    total.to_csv(ARQ_TOTAL_ARTISTA, sep=";", index=False)
    print(total)


# ----------------------------- Definição da DAG -----------------------------

with DAG(
    dag_id="pipeline_stream",
    description="AV-02 SSA - Pipeline de processamento do dataset musical",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    tags=["av-02", "ssa", "airflow"],
) as dag:

    # TASK-1: copia o arquivo original para entrada.csv
    task_1 = BashOperator(
        task_id="task_1_copia_arquivo",
        bash_command=f"cp {ARQ_ORIGEM} {ARQ_ENTRADA}",
    )

    # TASK-2: padroniza datas
    task_2 = PythonOperator(
        task_id="task_2_trata_datas",
        python_callable=tratar_datas,
    )

    # TASK-3: remove linhas com nome_musica vazio e empurra contagem via XCom
    task_3 = PythonOperator(
        task_id="task_3_remove_vazios",
        python_callable=remover_musicas_vazias,
    )

    # TASK-4: insere o total descartado na tabela descartados
    task_4 = SQLExecuteQueryOperator(
        task_id="task_4_insere_descartados",
        conn_id="postgres",
        sql="""
            INSERT INTO descartados (total)
            VALUES ({{ ti.xcom_pull(task_ids='task_3_remove_vazios',
                                    key='total_descartado') }});
        """,
    )

    # TASK-5: consulta os gêneros musicais (resultado vai pro XCom automático)
    task_5 = SQLExecuteQueryOperator(
        task_id="task_5_consulta_generos",
        conn_id="postgres",
        sql="SELECT id_genero, nome_genero FROM genero_musical;",
    )

    # TASK-6: enriquece o task3.csv com a coluna nome_genero
    task_6 = PythonOperator(
        task_id="task_6_enriquece_genero",
        python_callable=enriquecer_com_genero,
    )

    # TASK-7: média de avaliação por música
    task_7 = PythonOperator(
        task_id="task_7_media_avaliacao",
        python_callable=media_avaliacao_por_musica,
    )

    # TASK-8: total de músicas por artista
    task_8 = PythonOperator(
        task_id="task_8_total_artista",
        python_callable=total_musicas_por_artista,
    )

    # TASK-9: remove entrada.csv não importando se 7 e 8 falharam ou não
    task_9 = BashOperator(
        task_id="task_9_remove_entrada",
        bash_command=f"rm -f {ARQ_ENTRADA}",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # TASK-10: marca o fim do processamento (não faz nada)
    task_10 = EmptyOperator(task_id="task_10_fim")

    # ----------------------------- Orquestração -----------------------------
    task_1 >> task_2 >> task_3
    task_3 >> task_4 >> task_10
    [task_3, task_5] >> task_6 >> [task_7, task_8] >> task_9 >> task_10