# 🐘 JUMBOBOT – Tracker de Precios Jumbo Argentina

Seguimiento diario automatizado de precios del supermercado **Jumbo** (jumbo.com.ar).

## ¿Qué hace?

- 🕐 **Scraper diario** automático vía GitHub Actions (9 AM Argentina)
- 📦 ~383 categorías de Jumbo vía API VTEX Intelligent Search
- ⚡ Scraping paralelo (~5 minutos en total con 8 workers)
- 📊 Web con gráficos históricos, rankings y variaciones
- 🐦 Tweet resumen diario en X (opcional)

## Cómo funciona técnicamente

Jumbo usa **VTEX IO** con el endpoint de **Intelligent Search**:

```
GET https://www.jumbo.com.ar/api/io/_v/api/intelligent-search/product_search/category-3/{slug}
    ?from=0&to=49&sort=price:desc
```

El scraper obtiene el árbol de categorías dinámicamente:
```
GET https://www.jumbo.com.ar/api/catalog_system/pub/category/tree/3
```

Y recorre los 3 niveles: nivel1 (cat_principal) → nivel2 (cat_padre) → nivel3 (slug).

## Estructura del proyecto

```
JUMBOBOT/
├── jumbo_scraper.py              ← Scraper paralelo (8 workers)
├── analizar_precios_jumbo.py     ← Genera JSONs de historial y rankings
├── generar_web_jumbo.py          ← Genera docs/index.html (GitHub Pages)
├── tweetear_jumbo.py             ← Publica resumen diario en X/Twitter
├── requirements.txt
├── data/                         ← JSONs generados (histórico, gráficos, rankings)
├── docs/                         ← Sitio web estático (GitHub Pages)
├── output_jumbo/                 ← CSVs crudos del scraper (gitignore)
└── .github/workflows/
    ├── scraper_diario.yml        ← Cron diario 9 AM Argentina
    └── regenerar_web.yml         ← Trigger manual para regenerar la web
```

## Setup (5 minutos)

### 1. Subir el repo a GitHub

### 2. Agregar output_jumbo/ al .gitignore
Crear archivo `.gitignore` con:
```
output_jumbo/
__pycache__/
*.pyc
```

### 3. Habilitar GitHub Pages
- Settings → Pages → Branch: `main`, carpeta: `/docs`
- Tu web quedará en `https://TU_USUARIO.github.io/NOMBRE_REPO`

### 4. Primer scraping manual
- Actions → "Scraper Diario Jumbo" → **Run workflow**
- Tarda ~5 minutos (con scraper paralelo)

### 5. (Opcional) Tweets automáticos
Agregar en Settings → Secrets → Actions:
- `X_API_KEY`
- `X_API_SECRET`
- `X_ACCESS_TOKEN`
- `X_ACCESS_SECRET`

## Categorías

El scraper descarga el árbol de categorías automáticamente desde Jumbo.
Las categorías de nivel 1 (cat_principal) que usa el análisis son:

- Almacén
- Bebidas Con Alcohol
- Bebidas Sin Alcohol
- Frescos
- Congelados
- Limpieza
- Cuidado Personal

Si Jumbo agrega o renombra categorías, el scraper las captura automáticamente.
Solo actualizar `ORDEN_CATS` en `analizar_precios_jumbo.py` si cambian los nombres de nivel 1.

## Ajuste de workers

En `jumbo_scraper.py`:
```python
WORKERS = 8   # categorías en paralelo — bajar a 4-5 si hay muchos errores 429
```

## Licencia

MIT – Uso educativo / transparencia de precios. No afiliado con Cencosud/Jumbo.
