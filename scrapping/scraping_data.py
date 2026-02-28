import asyncio
import re

from pathlib import Path
from playwright.async_api import async_playwright

CITIES = [
"annecy-france",
"Agde-france",
"Aix-en-Provence-france",
"Aix-les-Bains-france",
"Ajaccio-france",
"Angers-france",
"Anglet-france",
"Annecy-france",
"Antibes-france",
"Arcachon-france",
"Argelès-sur-Mer-france",
"Arles-france",
"Avignon-france",
"Bandol-france",
"Bayonne-france",
"Biarritz-france",
"Biscarrosse-france",
"Bordeaux-france",
"Boulogne-Billancourt-france",
"Bourg-Saint-Maurice-france",
"Brest-france",
"Caen-france",
"Cagnes-sur-Mer-france",
"Canet-en-Roussillon-france",
"Cannes-france",
"Capbreton-france",
"Carcassonne-france",
"Chamonix-Mont-Blanc-france",
"Clermont-Ferrand-france",
"Colmar-france",
"Dijon-france",
"Fréjus-france",
"Gérardmer-france",
"Grenoble-france",
"Gruissan-france",
"Gujan-Mestras-france",
"Huez-france",
"Hyères-france",
"La-Baule-Escoublac-france",
"La-Ciotat-france",
"La-Grande-Motte-france",
"La-Plagne-Tarentaise-france",
"La-Rochelle-france",
"La-Seyne-sur-Mer-france",
"La-Teste-de-Buch-france",
"Lacanau-france",
"Le-Barcarès-france",
"Le-Grau-du-Roi-france",
"Le-Havre-france",
"Le-Lavandou-france",
"Le-Mans-france",
"Lège-Cap-Ferret-france",
"Les-Allues-france",
"Les-Belleville-france",
"Les-Deux-Alpes-france",
"Les-Sables-d'Olonne-france",
"Lille-france",
"Lyon-france",
"Marseille-france",
"Menton-france",
"Montpellier-france",
"Montreuil-france",
"Morzine-france",
"Mulhouse-france",
"Nancy-france",
"Nantes-france",
"Narbonne-france",
"Nice-france",
"Nimes-france",
"Orléans-france",
"Paris-france",
"Perpignan-france",
"Porto-Vecchio-france",
"Reims-france",
"Rennes-france",
"Roquebrune-sur-Argens-france",
"Rouen-france",
"Royan-france",
"Saint-Martin-france-france",
"Saint-Cyprien-france",
"Saint-Denis-france",
"Saint-Étienne-france",
"Saint-François-france",
"Saint-Gervais-les-Bains-france",
"Saint-Jean-de-Monts-france",
"Saint-Malo-france",
"Saint-Paul-france",
"Saint-Raphaël-france",
"Sainte-Anne-france",
"Sainte-Maxime-france",
"Sanary-sur-Mer-france",
"Sète-france",
"Six-Fours-les-Plages-france",
"Strasbourg-france",
"Tignes-france",
"Toulon-france",
"Toulouse-france",
"Tours-france",
"Vallauris-france",
"Vannes-france",
"Villeurbanne-france"
]

TYPE_FILE = [
    "Listings Data",
    "Past Calendar Rates",
    "Future Calendar Rates",
    "Reviews Data",
]

BASE = "https://www.airroi.com/data-portal/markets"
OUTDIR = Path("downloads")
OUTDIR.mkdir(exist_ok=True)

async def download_city(page, slug: str):
    url = f"{BASE}/{re.sub(r"[èéÉëç']","",slug).lower()}"
    print("Open:", url)
    await page.goto(url, wait_until="domcontentloaded")

    # attend que le bouton apparaisse
    await page.wait_for_selector("text=Download", timeout=30000)
    i = 0
    for file in TYPE_FILE:
    # télécharge

        container = page.locator("div.space-y-4")
        # card = container.filter(has_text=file)
        # print(f"found card {file} {card}")
        async with page.expect_download(timeout=60000) as dl_info:
            await container.locator("text=Download").nth(i).click()
            await page.locator('[data-format="csv"]').click()

        download = await dl_info.value
        filename =  f"{file.replace(' ', '_')}_{slug}.csv"
        dest = OUTDIR / filename
        await download.save_as(dest)
        print("Saved:", dest)
        i = i + 1

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(accept_downloads=True)
        page = await context.new_page()

        for slug in CITIES:
            await download_city(page, slug)

        await context.close()
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())