# Como ejecutar el Scraping

obtienen la passkey en reviews buscando bazaarvoice y triggereando el api bajando a los comentarios
![1773375768728](image/scraping/1773375768728.png)

luego en el archivo .env asignan la passkey a la variable BAZAARVOICE_PASSKEY

Ahora solo queda ejecutar el pipeline

```bash
python src/ingestion/scraper/sample_scraper.py
```
