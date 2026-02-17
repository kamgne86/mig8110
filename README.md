# mig8110

### Dataset

Le dataset `canada_products.parquet` est disponible dans ce repo.

### Charger le dataset

```python
import io
import zipfile
import requests
import pandas as pd

url = "https://raw.githubusercontent.com/adilblanco/mig8110/main/data/canada_products.parquet.zip"

r = requests.get(url)
zip_bytes = io.BytesIO(r.content)

with zipfile.ZipFile(zip_bytes) as z:
    parquet_name = z.namelist()[0]
    with z.open(parquet_name) as f:
        df = pd.read_parquet(f)

print(df.shape)
```
