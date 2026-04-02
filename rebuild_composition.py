"""
Reconstrói ibov_composition.py usando COTAHIST como fonte primária,
com os deltas confirmados como cross-check de janelas de entrada/saída.

Uso:
  cd ~/Downloads/ibov-breadth
  python3 rebuild_composition.py --cotahist ~/Downloads/COTAHIST
"""

import argparse, zipfile, sys
from pathlib import Path
from datetime import datetime

# ── Períodos ──────────────────────────────────────────────────────────────────
PERIODS = [
    ("2014-01-01","2014-04-30"), ("2014-05-05","2014-08-29"),
    ("2014-09-01","2014-12-31"), ("2015-01-02","2015-04-30"),
    ("2015-05-04","2015-08-28"), ("2015-09-07","2015-12-31"),
    ("2016-01-04","2016-04-29"), ("2016-05-02","2016-09-02"),
    ("2016-09-05","2016-12-30"), ("2017-01-02","2017-04-28"),
    ("2017-05-02","2017-09-01"), ("2017-09-04","2017-12-29"),
    ("2018-01-02","2018-04-27"), ("2018-05-02","2018-08-31"),
    ("2018-09-03","2018-12-28"), ("2019-01-02","2019-05-03"),
    ("2019-05-06","2019-08-30"), ("2019-09-02","2020-01-03"),
    ("2020-01-06","2020-05-01"), ("2020-05-04","2020-09-04"),
    ("2020-09-07","2020-12-31"), ("2021-01-04","2021-04-30"),
    ("2021-05-03","2021-09-03"), ("2021-09-06","2021-12-31"),
    ("2022-01-03","2022-04-29"), ("2022-05-02","2022-09-02"),
    ("2022-09-05","2022-12-30"), ("2023-01-02","2023-04-28"),
    ("2023-05-02","2023-09-01"), ("2023-09-04","2023-12-29"),
    ("2024-01-02","2024-05-03"), ("2024-05-06","2024-08-30"),
    ("2024-09-02","2025-01-03"), ("2025-01-06","2025-05-02"),
    ("2025-05-05","2025-08-29"), ("2025-09-01","2026-01-02"),
    ("2026-01-05","2026-04-30"),
]

# ── Âncora jan/2026 ───────────────────────────────────────────────────────────
ANCHOR = sorted([
    "ABEV3","ALOS3","ASAI3","AURE3","AXIA3","AXIA6","AXIA7","AZZA3",
    "B3SA3","BBAS3","BBDC3","BBDC4","BBSE3","BEEF3","BPAC11","BRAP4",
    "BRAV3","BRKM5","CEAB3","CMIG4","CMIN3","COGN3","CPFE3","CPLE3",
    "CSAN3","CSMG3","CSNA3","CURY3","CXSE3","CYRE3","CYRE4","DIRR3",
    "EGIE3","EMBJ3","ENEV3","ENGI11","EQTL3","FLRY3","GGBR4","GOAU4",
    "HAPV3","HYPE3","IGTI11","IRBR3","ISAE4","ITSA4","ITUB4","KLBN11",
    "LREN3","MBRF3","MGLU3","MOTV3","MRVE3","MULT3","NATU3","PCAR3",
    "PETR3","PETR4","POMO4","PRIO3","PSSA3","RADL3","RAIL3","RAIZ4",
    "RDOR3","RECV3","RENT3","RENT4","SANB11","SBSP3","SLCE3","SMFT3",
    "SUZB3","TAEE11","TIMS3","TOTS3","UGPA3","USIM5","VALE3","VAMO3",
    "VBBR3","VIVA3","VIVT3","WEGE3","YDUQ3",
])

# ── Normalização ──────────────────────────────────────────────────────────────
TO_CURRENT = {
    "ELET3":"AXIA3","ELET5":"AXIA5","ELET6":"AXIA6",
    "EMBR3":"EMBJ3","CCRO3":"MOTV3","NTCO3":"NATU3",
    "MRFG3":"MBRF3","BRFS3":"MBRF3",
    "RRRP3":"BRAV3","ENAT3":"BRAV3",
    "ARZZ3":"AZZA3","SOMA3":"AZZA3",
    "TRPL4":"ISAE4","IGTA3":"IGTI11",
    "ALSO3":"ALOS3","BVMF3":"B3SA3",
    "SUZB5":"SUZB3","FIBR3":"SUZB3",
}

def normalize(t):
    return TO_CURRENT.get(t, t)

# ── Universo COTAHIST ─────────────────────────────────────────────────────────
IBOV_UNIVERSE = set(list(TO_CURRENT.keys()) + ANCHOR + [
    "ALPA4","EZTC3","DXCO3","CIEL3","STBP3","JHSF3","CASH3","POSI3",
    "ENBR3","BIDI4","BPAN4","ECOR3","QUAL3","AMER3","GETT11","PETZ3",
    "SMTO3","CVCB3","BTOW3","LCAM3","GOLL4","VVAR11","SAPR11","CRFB3",
    "BRML3","BRPR3","SMLE3","TIMP3","AZUL4","GNDI3","LWSA3","AMOB3",
    "CTCA3","OIBR3","PDGR3","KLBN4","DTEX3","LLXL3","OGXP3","GFSA3",
    "EVEN3","DASA3","CESP6","CPLE6","BISA3","CZRS4","CTIP3","CRUZ3",
    "HGTX3","LAME4","PORT3","RSID3","KROT3","VVAR3","BHIA3","VIIA3",
    "SULA11","INBR32","BIDI3","HAPV3","JBSS3","NATU3","IRBR3","RAIL3",
    "RECV3","VAMO3","SLCE3","RRRP3","RDOR3","GNDI3",
])

# ── Deltas confirmados ────────────────────────────────────────────────────────
# Tickers no formato histórico da época.
# NOTA: IGTA3 não está em set/2020 (estava desde jan/2018, não reentrou)
DELTAS = [
    ("2025-09-01",["CEAB3","CURY3","CSMG3","AXIA7","RENT4","CYRE4"],["PETZ3","SMTO3","CVCB3"]),
    ("2025-05-05",["DIRR3","SMFT3"],["AMOB3","LWSA3"]),
    ("2025-01-06",["POMO4","PSSA3"],["ALPA4","EZTC3"]),
    ("2024-09-02",["AURE3","CXSE3","STBP3","AZZA3"],["DXCO3","ARZZ3","SOMA3","CIEL3"]),
    ("2024-05-06",["VAMO3","RECV3"],["JHSF3","CASH3"]),
    ("2024-01-02",["TRPL4"],["POSI3"]),
    ("2023-09-04",["RECV3","VAMO3"],["CASH3","ENBR3","BIDI4"]),
    ("2023-05-02",["IRBR3"],["BPAN4","ECOR3","QUAL3"]),      # IRBR3 reentrou
    ("2023-01-02",[],["POSI3","IRBR3","AMER3"]),             # IRBR3 saiu
    ("2022-09-05",["ARZZ3","RAIZ4","SMTO3"],["JHSF3"]),
    ("2022-05-02",["SLCE3"],[]),
    ("2022-01-03",["RRRP3","POSI3","CMIN3"],["GETT11"]),
    ("2021-09-06",["DXCO3","PETZ3","RDOR3","ALPA4","BPAN4","CASH3","BIDI4","GETT11"],[]),
    ("2021-05-03",["LWSA3","PRIO3","CSNA3"],["BTOW3","SMTO3","LCAM3"]),
    ("2021-01-04",["HAPV3","JHSF3"],["QUAL3"]),
    # set/2020: IGTA3 NÃO entrou aqui — já estava desde jan/2018
    ("2020-09-07",["NTCO3","VBBR3"],["MRFG3","LAME4","FLRY3"]),
    ("2020-05-04",["CPFE3","ENGI11","BEEF3"],["NATU3","CSMG3"]),
    ("2020-01-06",["CRFB3","HAPV3","TOTS3","ENEV3","SBSP3"],["BRML3"]),
    ("2019-09-02",["GNDI3","BPAC11"],["BRPR3","SMLE3"]),
    ("2019-05-06",["AZUL4","IRBR3"],["TIMP3","NATU3"]),      # IRBR3 entrou 1a vez
    ("2019-01-02",["RAIL3"],["SUZB5"]),
    ("2018-09-03",["COGN3","EVEN3"],["PDGR3","OIBR3"]),
    ("2018-05-02",["HAPV3","EQTL3"],["CSMG3","CTCA3"]),
    # jan/2018: IGTA3 entrou (comunicado B3 02/01/2018 confirmado)
    ("2018-01-02",["FLRY3","IGTA3","MGLU3","SAPR11","VVAR11"],["BRKM5","GOLL4"]),
    ("2017-09-04",["KLBN11","BRKM5","TAEE11"],["KLBN4","BRPR3"]),
    ("2017-05-02",["MULT3","SBSP3"],["GOLL4","CSMG3"]),
    ("2017-01-02",["EQTL3"],["OIBR3","PDGR3"]),
    ("2016-09-05",["WEGE3","RADL3"],["DTEX3","LLXL3"]),
    ("2016-05-02",["LREN3","HYPE3"],["OGXP3"]),
    ("2016-01-04",["SBSP3","CSAN3"],["GFSA3"]),
    ("2015-09-07",["FLRY3","EVEN3"],["LLXL3","PDGR3"]),
    ("2015-05-04",["KLBN4"],["DASA3"]),
    ("2015-01-02",["MGLU3"],["CESP6","CPLE6"]),
    ("2014-09-01",["CPLE6","CESP6","SMLE3","BVMF3"],["LLXL3","GFSA3","OGXP3"]),
    ("2014-05-05",[],["BISA3","CZRS4","CTIP3","GFSA3","LLXL3","BRML3","BRPR3"]),
    ("2014-01-01",["BISA3","CZRS4","CTIP3","LLXL3","BRML3","BRPR3",
                   "BTOW3","CESP6","CPLE6","CRUZ3","DASA3","DTEX3",
                   "ECOR3","GFSA3","HGTX3","LAME4","OGXP3","PDGR3",
                   "PORT3","RSID3","SMLE3"],[]),
]

def build_windows():
    """
    Para cada ticker normalizado, calcula lista de janelas [(entry, exit_or_None)].
    Suporta múltiplas entradas/saídas (ex: IRBR3 que entrou/saiu/reentrou).
    """
    events = {}
    for period_start, entradas, saidas in sorted(DELTAS):
        for t in entradas:
            tn = normalize(t)
            events.setdefault(tn, []).append((period_start, 'entry'))
        for t in saidas:
            tn = normalize(t)
            events.setdefault(tn, []).append((period_start, 'exit'))

    windows = {}
    for tn, evs in events.items():
        evs_sorted = sorted(evs)
        spans = []
        current_entry = None
        for date, kind in evs_sorted:
            if kind == 'entry':
                if current_entry is None:
                    current_entry = date
                # Dupla entrada sem saída intermédia: ignora (erro no delta)
            elif kind == 'exit':
                if current_entry is not None:
                    spans.append((current_entry, date))
                    current_entry = None
                else:
                    # Exit sem entry precedente neste loop: ticker estava no índice
                    # antes do início do nosso histórico. Cria span desde jan/2014
                    # APENAS se não há span anterior (evita duplicatas).
                    if not spans:
                        spans.append(("2014-01-01", date))
                    # Se já há spans: esta saída é órfã (ex: segunda saída de um
                    # ticker cuja primeira janela já foi fechada) — ignora.
        if current_entry is not None:
            spans.append((current_entry, None))

        windows[tn] = spans

    # Tickers da âncora sem eventos: assumem entrada em jan/2014
    for t in ANCHOR:
        tn = normalize(t)
        if tn not in windows:
            windows[tn] = [("2014-01-01", None)]

    return windows


def in_window(tn, period_start, windows):
    """Retorna True se o ticker estava no índice no período dado."""
    for entry, exit_p in windows.get(tn, []):
        if entry <= period_start and (exit_p is None or exit_p > period_start):
            return True
    return False


# ── Conversão para saída histórica ───────────────────────────────────────────
RENAMES_TO_HIST = [
    ("AXIA3",  "ELET3",  "2000-01-01","2025-11-10"),
    ("AXIA6",  "ELET6",  "2000-01-01","2025-11-10"),
    ("EMBJ3",  "EMBR3",  "2000-01-01","2025-11-03"),
    ("MOTV3",  "CCRO3",  "2000-01-01","2025-05-02"),
    ("NATU3",  "NTCO3",  "2019-12-18","2025-07-02"),
    ("MBRF3",  "MRFG3",  "2000-01-01","2025-09-23"),
    ("BRAV3",  "RRRP3",  "2000-01-01","2024-09-09"),
    ("AZZA3",  "ARZZ3",  "2000-01-01","2024-09-02"),
    ("ISAE4",  "TRPL4",  "2000-01-01","2024-11-18"),
    ("IGTI11", "IGTA3",  "2000-01-01","2021-09-06"),
    ("ALOS3",  "ALSO3",  "2000-01-01","2023-05-02"),
    ("B3SA3",  "BVMF3",  "2000-01-01","2017-03-30"),
    ("SUZB3",  "SUZB5",  "2000-01-01","2018-01-02"),
]

def to_output(curr, period_start):
    dt = datetime.strptime(period_start, "%Y-%m-%d")
    for c, h, s, e in RENAMES_TO_HIST:
        if curr == c:
            if datetime.strptime(s,"%Y-%m-%d") <= dt < datetime.strptime(e,"%Y-%m-%d"):
                return h
    return curr

# ── COTAHIST ──────────────────────────────────────────────────────────────────

def load_cotahist_year(zip_path, universe):
    data = {}
    try:
        with zipfile.ZipFile(zip_path) as zf:
            txt = [n for n in zf.namelist() if n.upper().endswith(".TXT")][0]
            with zf.open(txt) as f:
                for raw in f:
                    line = raw.decode("latin-1", errors="ignore")
                    if len(line) < 150 or line[:2] != "01": continue
                    if line[10:12] != "02" or line[24:27] != "010": continue
                    t = line[12:24].strip()
                    if t not in universe: continue
                    data.setdefault(t, set()).add(line[2:10])
    except Exception as e:
        print(f"  Erro: {e}")
    return data


def get_candidates(cotahist_by_year, start, end, min_pct):
    s_dt = datetime.strptime(start, "%Y-%m-%d")
    e_dt = datetime.strptime(end,   "%Y-%m-%d")
    all_dates, count = set(), {}
    for year, ydata in cotahist_by_year.items():
        if year < s_dt.year or year > e_dt.year: continue
        for t, dates in ydata.items():
            tn = normalize(t)
            for d in dates:
                try: dt = datetime.strptime(d, "%Y%m%d")
                except: continue
                if s_dt <= dt <= e_dt:
                    all_dates.add(d)
                    count[tn] = count.get(tn, 0) + 1
    if not all_dates: return set()
    threshold = int(len(all_dates) * min_pct)
    return {t for t, c in count.items() if c >= threshold}


# ── Main ──────────────────────────────────────────────────────────────────────

def write_py(periods, path):
    lines = ['"""\nIBOV composition history — rebuilt by rebuild_composition.py\n"""\n\n']
    lines.append("IBOV_COMPOSITION_HISTORY = [\n")
    for p in periods:
        ts = ',\n            '.join(f'"{t}.SA"' for t in p["tickers"])
        lines.append(
            f'    {{\n        "start": "{p["start"]}", "end": "{p["end"]}",\n'
            f'        "tickers": [\n            {ts}\n        ],\n    }},\n'
        )
    lines.append("]\n")
    path.write_text("".join(lines))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cotahist", required=True)
    ap.add_argument("--min-presence", type=float, default=0.80)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if not Path("data").exists():
        print("ERRO: rode de dentro de ~/Downloads/ibov-breadth/")
        sys.exit(1)

    cotahist_dir = Path(args.cotahist).expanduser()
    zips = sorted(cotahist_dir.glob("COTAHIST_A*.ZIP"))
    print(f"Carregando {len(zips)} arquivos COTAHIST...")
    cotahist_by_year = {}
    for zp in zips:
        year = int(zp.stem.replace("COTAHIST_A",""))
        print(f"  {zp.name}...", end=" ", flush=True)
        cotahist_by_year[year] = load_cotahist_year(zp, IBOV_UNIVERSE)
        print(f"{len(cotahist_by_year[year])} tickers")

    print("\nCalculando janelas de entrada/saída dos deltas...")
    windows = build_windows()

    print(f"Extraindo composição (presença mínima: {args.min_presence:.0%})...")
    results = []

    for start, end in PERIODS:
        if start == "2026-01-05":
            results.append({"start":start,"end":end,"tickers":ANCHOR})
            continue

        # Candidatos do COTAHIST (normalizados)
        candidates = get_candidates(cotahist_by_year, start, end, args.min_presence)

        # Tickers que os deltas confirmam que estavam neste período
        forced_in  = {tn for tn in windows if in_window(tn, start, windows)}
        # Tickers que os deltas confirmam que NÃO estavam
        forced_out = {tn for tn in windows if not in_window(tn, start, windows)}

        # COTAHIST filtrado: remove tickers que os deltas confirmam fora
        filtered = (candidates - forced_out)

        # União: cotahist filtrado + forced_in (cobre IPOs e presença baixa)
        final_norm = filtered | forced_in

        # Converte para tickers históricos de saída
        output = sorted({to_output(t, start) for t in final_norm})
        results.append({"start":start,"end":end,"tickers":output})

    expected = {
        "2014-01-01":72,"2022-01-03":93,"2022-05-02":92,"2022-09-05":92,
        "2023-01-02":88,"2023-05-02":86,"2023-09-04":86,
        "2024-01-02":87,"2025-01-06":87,"2026-01-05":85,
    }
    print(f"\n{'Início':12} {'Fim':12} {'N':>4}  Validação B3")
    print("-"*55)
    for p in results:
        n = len(p["tickers"])
        exp = expected.get(p["start"])
        diff = f"(B3:{exp} diff:{n-exp:+d})" if exp else ""
        note = "⚠" if (n < 55 or n > 100) else ""
        print(f"{p['start']:12} {p['end']:12} {n:>4}  {note} {diff}")

    if args.dry_run:
        print("\n(dry-run — nada gravado)")
        return

    out = Path("data/ibov_composition.py")
    if out.exists():
        bak = Path("data/ibov_composition_backup.py")
        bak.write_text(out.read_text())
        print(f"\nBackup → {bak}")

    write_py(results, out)
    print(f"\n✓ {out} escrito ({len(results)} períodos)")
    print("Próximo passo:")
    print("  python3 jobs/daily_update.py")


if __name__ == "__main__":
    main()
