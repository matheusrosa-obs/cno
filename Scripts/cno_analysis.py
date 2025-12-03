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
    return pl.read_parquet(str(path))

######## Carregando os dados brutos ########
df_cno = load_data("Dados/Processados/cno_tratado_filtrado.parquet")

df_cno.head()

######## Contagem de obras por município ########
df_obras_munic = (
    df_cno.group_by(["codigo_municipio", "nome_municipio", "uf", "destinacao", "categoria"])
    .agg(pl.len().alias("total_obras"))
    .sort("total_obras", descending=True)
)

df_obras_munic = df_obras_munic.with_columns(
    pl.col("codigo_municipio").cast(pl.Utf8)
)

df_obras_munic.head(10)

df_obras_munic.write_csv(_resolve_path("Dados/Processados/obras_por_municipio.csv"))

######## Metragem de obras por município ########
df_area_munic = (
    df_cno.group_by(["codigo_municipio", "nome_municipio", "uf", "destinacao", "categoria"])
    .agg(
        pl.sum("area_total")
        .round(2)
        .alias("total_metragem")
    )
    .sort("total_metragem", descending=True)
)

df_area_munic = df_area_munic.with_columns(
    pl.col("codigo_municipio").cast(pl.Utf8)
)

df_area_munic.head(10)

df_area_munic.write_csv(_resolve_path("Dados/Processados/metragem_por_municipio.csv"))