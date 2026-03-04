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
df = load_data("Dados/Brutos/cno.csv")

df_sc = df.filter(pl.col("Estado") == "SC")
df_sc_ceps = df_sc.select(pl.col("CEP").cast(pl.Utf8)).unique()
df_sc_25000_30000 = df_sc_ceps["CEP"].to_list()[25000:30000]
list_df_sc_25000_30000 = list(df_sc_25000_30000)

import time, json, requests, pathlib

TOK = "ac81e9513452c703a5a0e0155ba66170"
SLEEP = 1.1  # ~3 req/s
cache = pathlib.Path("cache_ceps_SC_4500_9000")
cache.mkdir(exist_ok=True)
s = requests.Session()
s.headers.update({"Authorization": f"Token token={TOK}"})

def get_cep(cep):
    key = cache / f"{cep}.json"
    if key.exists():
        return json.loads(key.read_text(encoding="utf-8"))
    for attempt in range(5):
        r = s.get("https://www.cepaberto.com/api/v3/cep", params={"cep": cep}, timeout=15)
        if r.status_code == 200:
            key.write_text(r.text, encoding="utf-8")
            return r.json()
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(2**attempt)  # backoff exponencial
            continue
        return None
    return None

# loop simples
from tqdm import tqdm
ceps = list_df_sc_25000_30000
rows = []
for cep in tqdm(ceps):
    data = get_cep(cep)
    if data:
        rows.append((cep, data.get("latitude"), data.get("longitude"), data.get("logradouro"), data.get('bairro'), data.get('complemento'), data.get('cidade'), data.get('estado')))
    time.sleep(SLEEP)