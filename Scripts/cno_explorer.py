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

df_cno = df_cno.filter(
    (pl.col("uf") == "SC")
)

######## Filtrando colunas ########
df_cno = df_cno.select([
    pl.col("cno"),
    (pl.col("cep")).cast(pl.Utf8).alias("cep"),
    (pl.col("codigo_municipio")).cast(pl.Utf8).alias("codigo_municipio"),
    pl.col("nome_municipio"),
    pl.col("bairro"),
    pl.col("area_total"),
    pl.col("ano_inicio"),
    pl.col("ano_final"),
    pl.col("situacao"),
    pl.col("categoria"),
    pl.col("destinacao"),
    pl.col("tipo_obra")
])

df_cno = df_cno.with_columns(
    pl.when(pl.col("situacao") == 2)
    .then(1)
    .otherwise(0)
    .alias("obra_ativa")
)

df_cno = df_cno.with_columns([
    pl.col("nome_municipio").str.to_titlecase(),
    pl.col("bairro").str.to_titlecase()
])

df_cno.head()

df_cno.write_parquet(
    _resolve_path("Dados/Processados/cno_explorer_sc.parquet")
)