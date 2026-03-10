# Como ejecutar el Scraping

Descargar esta extensión: EditThisCookie(V3)
![1773114895862](image/scraping/1773114895862.png)

Entrar a la pagina oficial de sephora: sephora.com
y una vez ahí, entrar a la parte de extensiones y oprimir EditThisCookie

![1773115059346](image/scraping/1773115059346.png)
![1773115090316](image/scraping/1773115090316.png)

Y presionar la flecha hacia abajo, en donde dice exportar, entonces les debe salir un mensaje diciendo: cookies copied to clipboard

Ahora en la carpeta src/ingestion/scraper crean un archivo llamado sephora_cookies.json, y ahí pegan lo del paso anterior

![1773115275018](image/scraping/1773115275018.png)

Luego de nuevo vuelven a sephora.com y abren cualquier producto, una vez estén en la página de ese producto entonces presionan f12, les debe salir algo como esto:

![1773115453417](image/scraping/1773115453417.png)

En la parte de Network, en el filtro escriben bazaarvoice y luego de esto hacen scroll hacia abajo hasta que llegan a los comentarios, entonces se debe actualizar las cosas que aparecieron en network, entonces entran a cualquier request y en payload copian el valor de la passkey

![1773115857617](image/scraping/1773115857617.png)

luego en el archivo .env asignan la passkey a la variable BV_PASSKEY

![1773115931301](image/scraping/1773115931301.png)

Ahora solo queda ejecutar el pipeline

```bash
python src/ingestion/pipeline_scraping.py --limit 10
```

El limit indica que tanto quieren descargar, normalmente esta parte se demora bastante.
