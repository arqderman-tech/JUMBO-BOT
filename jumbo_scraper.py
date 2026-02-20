"""
jumbo_scraper.py  (versión optimizada con concurrencia)
========================================================
JUMBOBOT – Scraper de precios Jumbo Argentina
Usa la API VTEX Intelligent Search:
  /api/io/_v/api/intelligent-search/product_search/category-3/{slug}

OPTIMIZACIONES vs versión original:
  - ThreadPoolExecutor: scrappea N categorías en paralelo (WORKERS = 8)
  - Páginas de cada categoría también en paralelo (tras conocer el total)
  - Delays mínimos entre páginas (el Retry con backoff cubre los 429)
  - Escritura thread-safe al CSV con threading.Lock
  - Estimación de tiempo restante en consola
"""

import requests
import pandas as pd
import time
import threading
import random
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL   = "https://www.jumbo.com.ar"
PAGE_SIZE  = 50
MAX_PAGES  = 40          # 50 × 40 = 2 000 prods/cat
WORKERS    = 8           # categorías en paralelo  ← ajustá si hay 429s frecuentes
PAGE_DELAY = 0.3         # delay entre páginas de UNA categoría (era 1.5 s)
OUTPUT_DIR = Path("output_jumbo")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Referer": "https://www.jumbo.com.ar/",
}


# ──────────────────────────────────────────────
# Sesión compartida (thread-safe con HTTPAdapter)
# ──────────────────────────────────────────────
def crear_sesion():
    s = requests.Session()
    retry = Retry(
        total=6,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        respect_retry_after_header=True,   # respeta el header Retry-After si lo manda Jumbo
    )
    s.mount("https://", HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20))
    try:
        s.get(f"{BASE_URL}/api/segments", headers=HEADERS, timeout=15)
    except Exception:
        pass
    return s


# ──────────────────────────────────────────────
# Árbol de categorías
# ──────────────────────────────────────────────
def obtener_categorias(session):
    url = f"{BASE_URL}/api/catalog_system/pub/category/tree/3"
    try:
        r = session.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        tree = r.json()
    except Exception as e:
        print(f"[ERROR] No se pudo obtener árbol de categorías: {e}")
        return []

    cats = []
    for nivel1 in tree:
        for nivel2 in nivel1.get("children", []):
            for nivel3 in nivel2.get("children", []):
                slug = nivel3["url"].rstrip("/").split("/")[-1]
                cats.append((
                    nivel1["name"],
                    nivel2["name"],
                    nivel3["name"],
                    slug,
                    nivel3["id"],
                ))
    return cats


# ──────────────────────────────────────────────
# Fetch de UNA página
# ──────────────────────────────────────────────
def _fetch_pagina(session, slug, desde):
    hasta = desde + PAGE_SIZE - 1
    url = (
        f"{BASE_URL}/api/io/_v/api/intelligent-search/product_search"
        f"/category-3/{slug}"
        f"?from={desde}&to={hasta}&sort=price%3Adesc"
    )
    r = session.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()


def _parsear_prods(data, cat_principal, cat_padre, cat_nombre, slug):
    fecha = datetime.now().strftime("%Y-%m-%d")
    filas = []
    for p in data.get("products", []):
        for sku in p.get("items", []):
            sellers = sku.get("sellers", [])
            if not sellers:
                continue
            offer = sellers[0].get("commertialOffer", {})
            precio_actual = offer.get("Price", 0)
            if not precio_actual:
                continue
            precio_regular = offer.get("ListPrice", 0)
            if precio_regular > precio_actual * 10:
                precio_regular = precio_actual
            filas.append({
                "fecha":          fecha,
                "product_id":     p.get("productId", ""),
                "sku_id":         sku.get("itemId", ""),
                "ean":            sku.get("ean", ""),
                "nombre":         sku.get("nameComplete") or p.get("productName", ""),
                "marca":          p.get("brand", ""),
                "cat_principal":  cat_principal,
                "cat_padre":      cat_padre,
                "categoria":      cat_nombre,
                "slug":           slug,
                "precio_actual":  precio_actual,
                "precio_regular": precio_regular,
                "disponible":     offer.get("AvailableQuantity", 0),
                "link":           f"{BASE_URL}{p.get('link', '')}",
            })
    return filas


# ──────────────────────────────────────────────
# Scrape completo de UNA categoría (páginas en paralelo)
# ──────────────────────────────────────────────
def scrape_categoria(slug, cat_nombre, cat_padre, cat_principal, session):
    # 1) Primera página para saber el total
    try:
        data0    = _fetch_pagina(session, slug, 0)
        total_api = data0.get("recordsFiltered", 0)
        prods0    = _parsear_prods(data0, cat_principal, cat_padre, cat_nombre, slug)
    except Exception as e:
        print(f"  [Error primera pág {slug}]: {e}")
        return [], 0

    if total_api <= PAGE_SIZE:
        return prods0, total_api

    # 2) Páginas restantes en paralelo
    offsets = range(PAGE_SIZE, min(total_api, PAGE_SIZE * MAX_PAGES), PAGE_SIZE)
    todos   = list(prods0)

    def fetch_offset(desde):
        time.sleep(random.uniform(0, PAGE_DELAY))   # pequeño jitter para no explotar
        try:
            data = _fetch_pagina(session, slug, desde)
            return _parsear_prods(data, cat_principal, cat_padre, cat_nombre, slug)
        except Exception as e:
            print(f"  [Error pág offset={desde} {slug}]: {e}")
            return []

    # Usamos un pool pequeño por categoría (no queremos recursión de pools)
    with ThreadPoolExecutor(max_workers=4) as ppool:
        for resultado in ppool.map(fetch_offset, offsets):
            todos.extend(resultado)

    return todos, total_api


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────
def main():
    OUTPUT_DIR.mkdir(exist_ok=True)
    ts           = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_filename = OUTPUT_DIR / f"jumbo_{ts}.csv"
    session      = crear_sesion()
    csv_lock     = threading.Lock()   # escritura thread-safe

    print("Obteniendo árbol de categorías…")
    categorias = obtener_categorias(session)
    if not categorias:
        print("No se pudo obtener categorías. Abortando.")
        return

    total_cats  = len(categorias)
    acum_skus   = 0
    completadas = 0
    t_inicio    = time.time()

    print(f"\n{'='*65}")
    print(f"  JUMBOBOT – Intelligent Search API  /category-3/{{slug}}")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  |  {total_cats} categorías  |  workers={WORKERS}")
    print(f"{'='*65}\n")

    lock_print = threading.Lock()
    lock_acum  = threading.Lock()

    def procesar(args):
        nonlocal acum_skus, completadas
        i, (cat_principal, cat_padre, cat_nombre, slug, cat_id) = args

        prods, total_api = scrape_categoria(slug, cat_nombre, cat_padre, cat_principal, session)

        # Escritura CSV thread-safe
        if prods:
            df = pd.DataFrame(prods)
            with csv_lock:
                header = not csv_filename.exists()
                df.to_csv(csv_filename, mode="a", index=False, header=header, encoding="utf-8-sig")

        # Acumuladores y progreso
        with lock_acum:
            acum_skus   += len(prods)
            completadas += 1
            n_comp       = completadas
            n_skus       = acum_skus

        elapsed    = time.time() - t_inicio
        eta_s      = (elapsed / n_comp) * (total_cats - n_comp) if n_comp else 0
        eta_str    = f"{int(eta_s//60)}m{int(eta_s%60):02d}s"
        label      = f"{cat_nombre[:30]} [{slug}]"

        with lock_print:
            if prods:
                print(f"[{i:03d}/{total_cats}] {label.ljust(50)} → {len(prods):4d} / {total_api} SKUs"
                      f"  [total: {n_skus}]  ETA: {eta_str}")
            else:
                print(f"[{i:03d}/{total_cats}] {label.ljust(50)} → sin datos  (API: {total_api})")

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        list(executor.map(procesar, enumerate(categorias, 1)))

    elapsed_total = time.time() - t_inicio
    print(f"\n{'='*65}")
    print(f"  FIN · {acum_skus} SKUs · {int(elapsed_total//60)}m{int(elapsed_total%60):02d}s · {csv_filename}")
    print(f"{'='*65}")


if __name__ == "__main__":
    main()
