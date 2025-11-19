######################################################
# TRATAMENTO DAS INFORMAÇÕES DO CNO
######################################################
import polars as pl
from pathlib import Path

######## Configurando o caminho para a pasta raiz do projeto ########
def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent

def _resolve_path(path: str | Path) -> Path:
    p = Path(path)
    return p if p.is_absolute() else _project_root() / p

def load_data(file_path: str | Path) -> pl.DataFrame:
    path = _resolve_path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {path}")
    return pl.read_csv(str(path), encoding="latin1")

######## Carregando os dados brutos ########
df_dados = load_data("Dados/Brutos/cno.csv")

df_dados.head()

######## Tratamento inicial dos dados ########
df_cno = (
    df_dados.select([
        pl.col("CNO").alias("cno"),
        pl.col("Código do Pais").alias("codigo_pais"),
        pl.col("Nome do pais").alias("nome_pais"),
        pl.col("Data de início").alias("data_inicio"),
        pl.col("Data de inicio da responsabilidade").alias("data_inicio_responsabilidade"),
        pl.col("Data de registro").alias("data_registro"),
        pl.col("CNO vinculado").alias("cno_vinculado"),
        pl.col("CEP").alias("cep"),
        pl.col("NI do responsável").alias("ni_responsavel"),
        pl.col("Nome empresarial").alias("nome_empresarial"),
        pl.col("Nome").alias("nome_obra"),
        pl.col("Código do municipio").alias("codigo_municipio"),
        pl.col("Nome do município").alias("nome_municipio"),
        pl.col("Tipo de logradouro").alias("tipo_logradouro"),
        pl.col("Logradouro").alias("logradouro"),
        pl.col("Número do logradouro").alias("numero_logradouro"),
        pl.col("Bairro").alias("bairro"),
        pl.col("Estado").alias("uf"),
        pl.col("Unidade de medida").alias("unidade_medida"),
        pl.col("Área total").alias("area_total"),
        pl.col("Situação").alias("situacao"),
        pl.col("Data da situação").alias("data_situacao"),
        pl.col("Código de localização").alias("codigo_localizacao")
    ])
)

df_cno.head()

df_cno = df_cno.with_columns(
    pl.col("data_registro").str.slice(0, 4).alias("ano_registro"),
    pl.col("data_inicio").str.slice(0, 4).alias("ano_inicio"),
    pl.col("cno").cast(pl.Utf8)
)

df_cno = df_cno.filter(
    (pl.col("ano_inicio") >= "2020"),
    (pl.col("situacao") == 2)
)

########################### DADOS DE AREAS ###########################
df_areas = load_data("Dados/Brutos/cno_areas.csv")

df_areas_filtro = df_areas.with_columns(
    pl.col("CNO").alias("cno").cast(pl.Utf8),
    pl.col("Categoria").alias("categoria"),
    pl.col("Destinação").alias("destinacao"),
    pl.col("Tipo de obra").alias("tipo_obra")
).select([
    "cno",
    "categoria",
    "destinacao",
    "tipo_obra"
])

df_areas_filtro.head()

df_cno = df_cno.join(df_areas_filtro, on="cno", how="left")

df_cno = df_cno.filter(
    (pl.col("categoria") == "Obra Nova") | (pl.col("categoria") == "Reforma")
)
df_cno.head()

df_cno.write_parquet(_resolve_path("Dados/Processados/cno_tratado_filtrado.parquet"))