"""
tweetear_jumbo.py
=================
Publica un resumen diario en X (Twitter) con los datos de Jumbo.
"""

import json, os, tweepy
from pathlib import Path
from datetime import datetime

DIR_DATA = Path("data")


def main():
    resumen_path = DIR_DATA / "resumen.json"
    if not resumen_path.exists():
        print("No hay resumen.json")
        return

    with open(resumen_path, encoding="utf-8") as f:
        r = json.load(f)

    var_dia  = r.get("variacion_dia")
    var_mes  = r.get("variacion_mes")
    total    = r.get("total_productos", 0)
    sube     = r.get("productos_subieron_dia", 0)
    baja     = r.get("productos_bajaron_dia", 0)
    fecha    = datetime.now().strftime("%d/%m/%Y")
    web_url  = os.environ.get("WEB_URL", "")

    if var_dia is None:
        print("Sin variación del día, no se tweetea.")
        return

    emoji_dia = "📈" if var_dia > 0 else "📉"
    signo_dia = "+" if var_dia > 0 else ""

    cats = r.get("categorias_dia", [])
    lineas_cats = ""
    for cat in cats[:4]:
        v = cat.get("variacion_pct_promedio", 0)
        e = "🔴" if v > 0 else ("🟢" if v < 0 else "⚪")
        s = "+" if v > 0 else ""
        lineas_cats += f"\n{e} {cat['categoria']}: {s}{v:.1f}%"

    tweet = (
        f"🐘 Jumbo — {fecha}\n"
        f"{emoji_dia} Variación hoy: {signo_dia}{var_dia:.2f}%\n"
        f"📦 {total} productos · ⬆{sube} subieron · ⬇{baja} bajaron"
        f"{lineas_cats}\n"
    )
    if var_mes is not None:
        s = "+" if var_mes > 0 else ""
        tweet += f"\n📆 30 días: {s}{var_mes:.2f}%"
    if web_url:
        tweet += f"\n🔗 {web_url}"

    if len(tweet) > 280:
        tweet = tweet[:277] + "..."

    client = tweepy.Client(
        consumer_key=os.environ["X_API_KEY"],
        consumer_secret=os.environ["X_API_SECRET"],
        access_token=os.environ["X_ACCESS_TOKEN"],
        access_token_secret=os.environ["X_ACCESS_SECRET"],
    )
    response = client.create_tweet(text=tweet)
    print(f"✅ Tweet publicado: {response.data['id']}")
    print(tweet)


if __name__ == "__main__":
    main()
