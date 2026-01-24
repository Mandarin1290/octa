# Extrahiere alle FX-Symbole, die in allen Timeframes vorhanden sind
import os

fx_dir = "/home/n-b/Octa/raw/FX_parquet"
timeframes = ["1D", "1H", "30M", "5M", "1M"]

# Hole alle Basissymbole aus 1D
syms_1d = set(f[:-8] for f in os.listdir(fx_dir) if f.endswith("_1D.parquet"))

complete_syms = []
for sym in sorted(syms_1d):
    if all(os.path.exists(os.path.join(fx_dir, f"{sym}_{tf}.parquet")) for tf in timeframes):
        complete_syms.append(sym)

with open("fx_complete_symbols.txt", "w") as f:
    for sym in complete_syms:
        f.write(f"{sym}\n")

print(f"Gefundene vollständige FX-Symbole: {len(complete_syms)}")
