######################################################
# TRATAMENTO DAS INFORMAÇÕES DO CNO
######################################################
import polars as pl
from pathlib import Path
import matplotlib.pyplot as plt

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

df_areas = load_data("Dados/Brutos/cno_areas.csv")

df_areas.head()

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

df_cno = df_cno.with_columns(
    pl.col("data_registro").str.slice(0, 4).alias("ano_registro"),
    pl.col("data_inicio").str.slice(0, 4).alias("ano_inicio"),
    pl.col("cno").cast(pl.Utf8)
)

######## Join com as áreas do CNO ########
df_cno = df_cno.join(
    df_areas.select([
        pl.col("CNO").alias("cno").cast(pl.Utf8),
        pl.col("Categoria").alias("categoria"),
        pl.col("Destinação").alias("destinacao"),
        pl.col("Tipo de obra").alias("tipo_obra")
    ]),
    on="cno",
    how="left"
)

######## Filtros ########
df_cno = df_cno.filter(
    pl.col("ano_inicio") >= "2010",
    pl.col("uf") == "SC",
    pl.col("categoria") == "Obra Nova"
)

df_cno.head()

######## Análise exploratória inicial ########
# Obras novas iniciadas por ano
df_cno_inicios_ano = df_cno.group_by("ano_inicio").agg(
    pl.count().alias("total_inicios")
).sort("ano_inicio")

fig = plt.figure(figsize=(10, 6))

plt.bar(
    df_cno_inicios_ano["ano_inicio"].to_list(),
    df_cno_inicios_ano["total_inicios"].to_list()
)

plt.title("Inícios de obras por ano em SC (2010-2025)")
plt.xticks(rotation=90)
plt.show()

# Obras novas iniciadas por ano e destinação
df_cno_inicios_ano_dest = df_cno.group_by(["ano_inicio", "destinacao"]).agg(
    pl.count().alias("total_inicios")
).sort(["ano_inicio", "destinacao"])

df_cno_inicios_ano_dest = df_cno_inicios_ano_dest.pivot(
    values="total_inicios",
    index="ano_inicio",
    columns="destinacao"
).sort("ano_inicio")

df_cno_inicios_ano_dest.head()

# Metragem de obras novas iniciadas por ano
df_cno_area_ano = df_cno.group_by("ano_inicio").agg(
    pl.sum("area_total").alias("total_area_m2")
).sort("ano_inicio")

fig = plt.figure(figsize=(10, 6))

plt.bar(
    df_cno_area_ano["ano_inicio"].to_list(),
    df_cno_area_ano["total_area_m2"].to_list()
)

plt.title("Metragem de obras novas iniciadas por ano em SC (2010-2025)")
plt.xticks(rotation=90)
plt.show()

# Metragem de obras novas iniciadas por ano e destinação
df_cno_area_ano_dest = df_cno.group_by(["ano_inicio", "destinacao"]).agg(
    pl.sum("area_total").alias("total_area_m2")
).sort(["ano_inicio", "destinacao"])

df_cno_area_ano_dest = df_cno_area_ano_dest.pivot(
    values="total_area_m2",
    index="ano_inicio",
    columns="destinacao"
).sort("ano_inicio")

df_cno_area_ano_dest.head()


######## Exportação das planilhas ########
df_cno_inicios_ano.write_excel(
    _resolve_path("Dados/Processados/ind_competitividade/inicios_ano.xlsx")
)

df_cno_inicios_ano_dest.write_excel(
    _resolve_path("Dados/Processados/ind_competitividade/inicios_ano_destinacao.xlsx")
)

df_cno_area_ano.write_excel(
    _resolve_path("Dados/Processados/ind_competitividade/area_ano.xlsx")
)

df_cno_area_ano_dest.write_excel(
    _resolve_path("Dados/Processados/ind_competitividade/area_ano_destinacao.xlsx")
)