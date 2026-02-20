"""
analizar_precios_jumbo.py
=========================
Lee output_jumbo/*.csv del día, guarda histórico en
data/precios_compacto.csv y genera los JSONs para la web.

- Una fila por producto por día
- Índices % acumulados día a día
- Comparaciones vs día/7d/30d/6m/1y
- Categorías principales (usa cat_principal del CSV directamente)
"""

import json
import glob
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import sys

DIR_DATA         = Path("data")
PRECIOS_COMPACTO = DIR_DATA / "precios_compacto.csv"

# Estas categorías deben coincidir exactamente con los nombres nivel-1
# que devuelve el árbol de Jumbo (el scraper los guarda en cat_principal)
ORDEN_CATS = [
    "Almacén",
    "Bebidas",
    "Congelados",
    "Lácteos",
    "Quesos y Fiambres",
    "Frutas y Verduras",
    "Carnes",
    "Rotiseria",
    "Panaderia y Pasteleria",
    "Limpieza",
    "Perfumería",
    "Mascotas",
    "Hogar y textil",
    "Mundo Bebe",
    "Electro",
    "Tiempo Libre",
]

PERIODOS = {"7d": 7, "30d": 30, "6m": 180, "1y": 365}


def cargar_csvs_hoy():
    hoy = datetime.now().strftime("%Y%m%d")
    patron = f"output_jumbo/jumbo_{hoy}*.csv"
    dfs = []
    for archivo in glob.glob(patron):
        try:
            df = pd.read_csv(archivo, encoding="utf-8-sig")
            dfs.append(df)
            print(f"  Cargado: {archivo} ({len(df)} prods)")
        except Exception as e:
            print(f"  ERROR cargando {archivo}: {e}")
    if not dfs:
        print("ERROR: No se encontraron CSVs de hoy.")
        return None
    return pd.concat(dfs, ignore_index=True)


def preparar_df_dia(df_raw, fecha_str):
    df = df_raw.copy()
    df["fecha"] = fecha_str

    # Usar cat_principal directamente del CSV (viene del nivel 1 del árbol de Jumbo)
    # Si por algún motivo está vacía, marcamos como "Otros"
    df["cat_principal"] = df["cat_principal"].fillna("Otros").replace("", "Otros")

    df["precio_actual"] = pd.to_numeric(df["precio_actual"], errors="coerce")
    df = df.dropna(subset=["precio_actual"])
    df = df[df["precio_actual"] > 0]
    df["sku_id"] = df["sku_id"].astype(str)
    df = df.drop_duplicates(subset=["sku_id", "fecha"], keep="last")
    return df[["sku_id", "nombre", "marca", "categoria", "cat_principal",
               "precio_actual", "precio_regular", "fecha"]]


def actualizar_historico(df_hoy):
    DIR_DATA.mkdir(exist_ok=True)
    if PRECIOS_COMPACTO.exists():
        df_hist = pd.read_csv(PRECIOS_COMPACTO, encoding="utf-8-sig",
                              dtype={"sku_id": str})
        fecha_hoy = df_hoy["fecha"].iloc[0]
        df_hist = df_hist[df_hist["fecha"] != fecha_hoy]
        df_total = pd.concat([df_hist, df_hoy], ignore_index=True)
    else:
        df_total = df_hoy
    df_total.to_csv(PRECIOS_COMPACTO, index=False, encoding="utf-8-sig")
    print(f"  Histórico actualizado: {len(df_total)} filas totales")
    return df_total


def calcular_variacion_periodo(df, dias):
    fechas = sorted(df["fecha"].unique())
    if len(fechas) < 2:
        return None
    fecha_hoy  = fechas[-1]
    fecha_ref  = (datetime.strptime(fecha_hoy, "%Y-%m-%d") - timedelta(days=dias)).strftime("%Y-%m-%d")
    fechas_disp = [f for f in fechas if f <= fecha_ref]
    if not fechas_disp:
        return None
    fecha_ref_real = fechas_disp[-1]

    df_hoy = df[df["fecha"] == fecha_hoy][["sku_id", "precio_actual"]].rename(columns={"precio_actual": "p_hoy"})
    df_ref  = df[df["fecha"] == fecha_ref_real][["sku_id", "precio_actual"]].rename(columns={"precio_actual": "p_ref"})

    merged = df_hoy.merge(df_ref, on="sku_id")
    merged = merged[(merged["p_hoy"] > 0) & (merged["p_ref"] > 0)]
    if merged.empty:
        return None
    merged["diff_pct"] = (merged["p_hoy"] - merged["p_ref"]) / merged["p_ref"] * 100
    return round(merged["diff_pct"].mean(), 2)


def calcular_graficos(df, dias_max):
    fechas = sorted(df["fecha"].unique())
    if len(fechas) < 2:
        return {"total": [], "categorias": {}}

    fecha_inicio = (datetime.strptime(fechas[-1], "%Y-%m-%d") - timedelta(days=dias_max)).strftime("%Y-%m-%d")
    fechas_rango = [f for f in fechas if f >= fecha_inicio]
    if len(fechas_rango) < 2:
        fechas_rango = fechas[-min(len(fechas), dias_max):]

    fecha_base = fechas_rango[0]
    serie_total = [{"fecha": fecha_base, "pct": 0.0}]

    # Solo incluir categorías presentes en los datos
    cats_presentes = [c for c in ORDEN_CATS if c in df["cat_principal"].unique()]
    series_cats = {cat: [{"fecha": fecha_base, "pct": 0.0}] for cat in cats_presentes}
    pct_acum_total = 0.0
    pct_acum_cats  = {cat: 0.0 for cat in cats_presentes}

    for i in range(1, len(fechas_rango)):
        f_ant = fechas_rango[i - 1]
        f_act = fechas_rango[i]

        df_ant = df[df["fecha"] == f_ant][["sku_id", "precio_actual", "cat_principal"]]
        df_act = df[df["fecha"] == f_act][["sku_id", "precio_actual", "cat_principal"]]
        merged = df_ant.merge(df_act, on="sku_id", suffixes=("_ant", "_act"))
        merged = merged[(merged["precio_actual_ant"] > 0) & (merged["precio_actual_act"] > 0)]

        if not merged.empty:
            merged["diff_pct"] = (merged["precio_actual_act"] - merged["precio_actual_ant"]) / merged["precio_actual_ant"] * 100
            pct_acum_total += merged["diff_pct"].mean()

        serie_total.append({"fecha": f_act, "pct": round(pct_acum_total, 2)})

        for cat in cats_presentes:
            sub = merged[merged["cat_principal_ant"] == cat] if not merged.empty else pd.DataFrame()
            if not sub.empty:
                pct_acum_cats[cat] += sub["diff_pct"].mean()
            series_cats[cat].append({"fecha": f_act, "pct": round(pct_acum_cats[cat], 2)})

    return {"total": serie_total, "categorias": series_cats}


def calcular_ranking(df, dias, top_n=20):
    fechas = sorted(df["fecha"].unique())
    if len(fechas) < 2:
        return [], []
    fecha_hoy   = fechas[-1]
    fecha_ref   = (datetime.strptime(fecha_hoy, "%Y-%m-%d") - timedelta(days=dias)).strftime("%Y-%m-%d")
    fechas_disp = [f for f in fechas if f <= fecha_ref]
    if not fechas_disp:
        return [], []
    fecha_ref_real = fechas_disp[-1]

    df_hoy = df[df["fecha"] == fecha_hoy][["sku_id", "nombre", "marca", "categoria", "precio_actual"]]
    df_ref  = df[df["fecha"] == fecha_ref_real][["sku_id", "precio_actual"]].rename(columns={"precio_actual": "precio_ref"})
    merged = df_hoy.merge(df_ref, on="sku_id")
    merged = merged[(merged["precio_actual"] > 0) & (merged["precio_ref"] > 0)]
    if merged.empty:
        return [], []

    merged["diff_pct"] = (merged["precio_actual"] - merged["precio_ref"]) / merged["precio_ref"] * 100
    merged = merged.rename(columns={"precio_actual": "precio_hoy"})

    sube = merged.nlargest(top_n, "diff_pct")[["sku_id", "nombre", "marca", "categoria", "precio_hoy", "precio_ref", "diff_pct"]]
    baja = merged.nsmallest(top_n, "diff_pct")[["sku_id", "nombre", "marca", "categoria", "precio_hoy", "precio_ref", "diff_pct"]]
    return sube.to_dict("records"), baja.to_dict("records")


def calcular_resumen(df):
    fechas = sorted(df["fecha"].unique())
    if not fechas:
        return {}
    fecha_hoy = fechas[-1]
    df_hoy = df[df["fecha"] == fecha_hoy]

    var_dia  = calcular_variacion_periodo(df, 1)
    var_mes  = calcular_variacion_periodo(df, 30)
    var_anio = calcular_variacion_periodo(df, 365)

    sube = baja = igual = 0
    if len(fechas) >= 2:
        fecha_ant = fechas[-2]
        df_ant = df[df["fecha"] == fecha_ant][["sku_id", "precio_actual"]].rename(columns={"precio_actual": "p_ant"})
        merged = df_hoy[["sku_id", "precio_actual"]].merge(df_ant, on="sku_id")
        merged = merged[(merged["precio_actual"] > 0) & (merged["p_ant"] > 0)]
        if not merged.empty:
            sube  = int((merged["precio_actual"] > merged["p_ant"]).sum())
            baja  = int((merged["precio_actual"] < merged["p_ant"]).sum())
            igual = int((merged["precio_actual"] == merged["p_ant"]).sum())

    cats_dia = []
    cats_presentes = [c for c in ORDEN_CATS if c in df["cat_principal"].unique()]
    for cat in cats_presentes:
        df_cat = df[df["cat_principal"] == cat]
        v = calcular_variacion_periodo(df_cat, 1)
        n_sube = n_baja = n_total = 0
        if len(fechas) >= 2:
            fecha_ant = fechas[-2]
            dc_hoy = df_cat[df_cat["fecha"] == fecha_hoy][["sku_id", "precio_actual"]]
            dc_ant = df_cat[df_cat["fecha"] == fecha_ant][["sku_id", "precio_actual"]].rename(columns={"precio_actual": "p_ant"})
            m = dc_hoy.merge(dc_ant, on="sku_id")
            m = m[(m["precio_actual"] > 0) & (m["p_ant"] > 0)]
            if not m.empty:
                n_sube  = int((m["precio_actual"] > m["p_ant"]).sum())
                n_baja  = int((m["precio_actual"] < m["p_ant"]).sum())
                n_total = len(m)
        cats_dia.append({
            "categoria":               cat,
            "variacion_pct_promedio":  v or 0,
            "productos_subieron":      n_sube,
            "productos_bajaron":       n_baja,
            "total_productos":         n_total,
        })

    return {
        "fecha_actualizacion":     fecha_hoy,
        "total_productos":         int(len(df_hoy)),
        "variacion_dia":           var_dia,
        "variacion_mes":           var_mes,
        "variacion_anio":          var_anio,
        "productos_subieron_dia":  sube,
        "productos_bajaron_dia":   baja,
        "productos_sin_cambio_dia": igual,
        "categorias_dia":          cats_dia,
    }


def guardar_json(datos, nombre):
    DIR_DATA.mkdir(exist_ok=True)
    ruta = DIR_DATA / nombre
    with open(ruta, "w", encoding="utf-8") as f:
        json.dump(datos, f, ensure_ascii=False, indent=2)
    print(f"  JSON guardado: {ruta}")


def main():
    print(f"\n{'='*60}")
    print(f" ANALIZAR PRECIOS JUMBO")
    print(f" {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")

    DIR_DATA.mkdir(exist_ok=True)
    fecha_hoy = datetime.now().strftime("%Y-%m-%d")

    print("1. Cargando CSVs del día...")
    df_raw = cargar_csvs_hoy()
    if df_raw is None:
        sys.exit(1)

    print(f"\n2. Preparando datos ({len(df_raw)} filas)...")
    df_hoy = preparar_df_dia(df_raw, fecha_hoy)
    print(f"   {len(df_hoy)} productos válidos")
    print(f"   Categorías encontradas: {sorted(df_hoy['cat_principal'].unique())}")

    print("\n3. Actualizando histórico...")
    df_hist = actualizar_historico(df_hoy)

    print("\n4. Calculando resumen...")
    resumen = calcular_resumen(df_hist)
    guardar_json(resumen, "resumen.json")

    print("\n5. Calculando gráficos...")
    graficos = {}
    for key, dias in PERIODOS.items():
        graficos[key] = calcular_graficos(df_hist, dias)
    guardar_json(graficos, "graficos.json")

    print("\n6. Calculando rankings...")
    rank_sube_dia,  rank_baja_dia  = calcular_ranking(df_hist, 1)
    rank_sube_mes,  rank_baja_mes  = calcular_ranking(df_hist, 30)
    rank_sube_anio, rank_baja_anio = calcular_ranking(df_hist, 365)
    guardar_json(rank_sube_dia,  "ranking_dia.json")
    guardar_json(rank_baja_dia,  "ranking_baja_dia.json")
    guardar_json(rank_sube_mes,  "ranking_mes.json")
    guardar_json(rank_sube_anio, "ranking_anio.json")

    # ranking_baja_dia también en resumen para el tweet
    resumen["ranking_baja_dia"] = rank_baja_dia[:10]
    guardar_json(resumen, "resumen.json")

    print(f"\n{'='*60}")
    print(f" ANÁLISIS COMPLETADO")
    v = resumen.get("variacion_dia")
    if v is not None:
        print(f" Variación del día: {'+'if v>0 else ''}{v:.2f}%")
    print(f" Productos relevados: {resumen.get('total_productos', 0)}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
