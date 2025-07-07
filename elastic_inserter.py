from elasticsearch import Elasticsearch
import json
from datetime import datetime

es = Elasticsearch("http://localhost:9200")  # Modifie si besoin

def index_log(log: dict):
    date_suffix = datetime.now().strftime("%Y.%m.%d")
    index_name = f"logs-{date_suffix}"
    es.index(index=index_name, document=log)

if __name__ == "__main__":
    with open("app.log", "r") as f:
        for line in f:
            try:
                log = json.loads(line)
                index_log(log)
            except Exception as e:
                print(f"❌ Erreur lors de l'insertion : {e}")
