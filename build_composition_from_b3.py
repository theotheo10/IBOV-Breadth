"""
Reconstrói data/ibov_composition.py com composições oficiais da B3.
Cobre as viradas de jan/2022 em diante — únicas acessíveis online.

Uso:
  cd ~/Downloads/ibov-fixed
  python3 build_composition_from_b3.py

Demora ~2-3 minutos.
"""

import re, sys, time, zipfile, requests, openpyxl
from io import BytesIO
from pathlib import Path

COMPOSITION_PATH = Path("data/ibov_composition.py")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://borainvestir.b3.com.br/",
}

# Cada virada: (start, end, url_do_artigo_borainvestir)
# jan/2026 já temos pelo XLSX que você baixou manualmente
PERIODS = [
    # 2025
    ("2025-09-01", "2026-01-02",
     "https://borainvestir.b3.com.br/tipos-de-investimentos/renda-variavel/indices/nova-carteira-do-ibovespa-b3-entra-em-vigor-hoje-com-84-ativos-confira-as-novidades/"),
    ("2025-05-05", "2025-08-29",
     "https://borainvestir.b3.com.br/tipos-de-investimentos/renda-variavel/indices/b3-anuncia-nova-carteira-do-ibovespa-veja-o-que-muda-na-composicao-do-indice/"),
    ("2025-01-06", "2025-05-02",
     "https://borainvestir.b3.com.br/tipos-de-investimentos/renda-variavel/indices/nova-carteira-do-ibovespa-b3-de-janeiro-divulgada-confira-quem-entra-e-quem-sai/"),
    # 2024
    ("2024-09-02", "2025-01-03",
     "https://borainvestir.b3.com.br/tipos-de-investimentos/renda-variavel/etfs/nova-carteira-do-ibovespa-passa-a-valer-hoje-veja-o-que-muda/"),
    ("2024-05-06", "2024-08-30",
     "https://www.b3.com.br/pt_br/noticias/nova-carteira-do-ibovespa-b3-tem-86-papeis.htm"),
    ("2024-01-02", "2024-05-03",
     "https://www.b3.com.br/pt_br/noticias/nova-carteira-do-ibovespa-b3-de-janeiro-a-abril-tem-87-papeis.htm"),
    # 2023
    ("2023-09-04", "2023-12-29",
     "https://www.b3.com.br/pt_br/noticias/b3-anuncia-nova-carteira-do-ibovespa-ate-dezembro.htm"),
    ("2023-05-02", "2023-09-01",
     "https://www.b3.com.br/pt_br/noticias/nova-carteira-do-ibovespa-b3-tem-89-papeis.htm"),
    ("2023-01-02", "2023-04-28",
     "https://borainvestir.b3.com.br/noticias/mercado/nova-carteira-do-ibovespa-b3-tem-89-papeis-positivo-e-irb-brasil-saem/"),
    # 2022
    ("2022-09-05", "2022-12-30",
     "https://www.b3.com.br/pt_br/noticias/nova-carteira-do-ibovespa-b3-tem-89-papeis.htm"),
    ("2022-05-02", "2022-09-02",
     "https://www.b3.com.br/pt_br/noticias/carteira-ibovespa-e-demais-indices.htm"),
    ("2022-01-03", "2022-04-29",
     "https://www.b3.com.br/pt_br/noticias/nova-carteira-do-ibovespa-b3-tem-89-papeis.htm"),
]

def fetch(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        return r.text if r.status_code == 200 else None
    except Exception as e:
        print(f"    Erro HTTP: {e}")
        return None

def find_xlsx_url(html, base_url):
    """Extrai qualquer link para XLSX ou ZIP da página."""
    patterns = [
        r'href="(https?://[^"]*\.xlsx[^"]*)"',
        r'href="(https?://[^"]*\.zip[^"]*)"',
        r'href="(/data/files/[^"]*\.xlsx[^"]*)"',
        r'href="(/data/files/[^"]*\.zip[^"]*)"',
        r'(https://nam\d+\.safelinks\.protection\.outlook\.com/[^"\'>\s]+)',
        r'(https://s\d+\.imxsnd\d+\.com/link\.php[^"\'>\s]+)',
        r'(https://app\.i-maxpr\.com/x/[^"\'>\s]+)',
    ]
    from urllib.parse import urlparse
    base = urlparse(base_url)
    for pat in patterns:
        for m in re.findall(pat, html, re.IGNORECASE):
            url = m if m.startswith('http') else f"{base.scheme}://{base.netloc}{m}"
            return url
    return None

def download_xlsx(url):
    """Baixa URL e tenta parsear como XLSX ou ZIP contendo XLSX."""
    try:
        r = requests.get(url, headers=HEADERS, timeout=30, allow_redirects=True)
        if r.status_code != 200:
            print(f"    Download falhou: HTTP {r.status_code}")
            return None
        content = r.content
        # Tenta como XLSX direto
        if content[:4] == b'PK\x03\x04':
            try:
                return parse_xlsx(BytesIO(content))
            except Exception:
                pass
            # Tenta como ZIP
            try:
                with zipfile.ZipFile(BytesIO(content)) as zf:
                    xlsx_files = [n for n in zf.namelist() if n.upper().endswith('.XLSX')]
                    if not xlsx_files:
                        return None
                    # Prefere o arquivo IBOV
                    ibov = [x for x in xlsx_files if 'IBOV' in x.upper()] or xlsx_files
                    with zf.open(ibov[0]) as f:
                        return parse_xlsx(BytesIO(f.read()))
            except Exception as e:
                print(f"    ZIP parse erro: {e}")
        return None
    except Exception as e:
        print(f"    Download erro: {e}")
        return None

def parse_xlsx(f):
    wb = openpyxl.load_workbook(f, read_only=True, data_only=True)
    # Tenta aba IBOV primeiro
    ws = None
    for name in wb.sheetnames:
        if 'IBOV' in name.upper():
            ws = wb[name]
            break
    ws = ws or wb.active

    tickers = []
    for i, row in enumerate(ws.iter_rows(values_only=True)):
        if i < 2: continue
        if not row[0]: continue
        code = str(row[0]).strip()
        if (len(code) >= 4 and code[0].isalpha()
                and not any(code.startswith(x) for x in ['Preg', 'Quant', 'Valor', 'CODI'])):
            tickers.append(code + '.SA')
    return sorted(tickers) if len(tickers) >= 30 else None

def get_tickers(start, url):
    print(f"  Buscando página: {url[:80]}")
    html = fetch(url)
    if not html:
        return None

    xlsx_url = find_xlsx_url(html, url)
    if not xlsx_url:
        print(f"  Nenhum link de download encontrado")
        return None

    print(f"  Download: {xlsx_url[:80]}")
    tickers = download_xlsx(xlsx_url)
    if tickers:
        print(f"  ✓ {len(tickers)} tickers")
    else:
        print(f"  ✗ falhou ao parsear")
    return tickers

def main():
    if not Path("data").exists():
        print("ERRO: rode de dentro de ~/Downloads/ibov-fixed/")
        sys.exit(1)

    # Lê ibov_composition.py atual para manter os períodos históricos (pre-2022)
    current = COMPOSITION_PATH.read_text()

    # Extrai os períodos históricos que já existem (antes de 2022)
    # Vamos cortar no primeiro período >= 2022
    cut_marker = '    {\n        "start": "2022-01-03"'
    cut_marker2 = '    {\n        "start": "2022-01'
    
    # Encontra onde começa o primeiro período de 2022+
    cut_pos = current.find('    {\n        "start": "2022')
    if cut_pos == -1:
        cut_pos = current.find('    {\n        "start": "2021')
        if cut_pos == -1:
            print("ERRO: não encontrei marco de corte em ibov_composition.py")
            sys.exit(1)

    historical_part = current[:cut_pos]  # mantém header + períodos pré-2022

    print(f"Mantendo histórico até o marco de corte (pos {cut_pos})")
    print(f"Buscando {len(PERIODS)} períodos de 2022 em diante...\n")

    new_entries = []
    failed = []

    for start, end, url in PERIODS:
        print(f"\n{'='*60}")
        print(f"Período: {start} → {end}")
        tickers = get_tickers(start, url)
        if tickers:
            new_entries.append((start, end, tickers))
        else:
            failed.append((start, end))
        time.sleep(1.5)

    print(f"\n{'='*60}")
    print(f"OK: {len(new_entries)} | Falhou: {len(failed)}")
    if failed:
        print("Falhas:")
        for s, e in failed:
            print(f"  {s} → {e}")

    if not new_entries:
        print("Nenhum período recuperado. Verifique a conexão.")
        sys.exit(1)

    # Monta o arquivo final: histórico + novos + jan/2026 que já está no arquivo atual
    lines = historical_part.rstrip()
    if not lines.endswith(','):
        lines += ','

    for start, end, tickers in sorted(new_entries):
        ticker_lines = ',\n            '.join(f'"{t}"' for t in tickers)
        lines += f'\n    {{\n        "start": "{start}", "end": "{end}",\n        "tickers": [\n            {ticker_lines}\n        ],\n    }},'

    # Adiciona o período jan/2026 que já está validado
    jan2026_start = current.find('    {\n        "start": "2026-01-05"')
    if jan2026_start != -1:
        jan2026_block = current[jan2026_start:].rstrip()
        # Remove o ] final para reaproveitar
        jan2026_block = jan2026_block.rstrip(']').rstrip()
        lines += '\n' + jan2026_block

    lines += '\n]'

    COMPOSITION_PATH.write_text(lines)
    print(f"\n✓ ibov_composition.py atualizado")
    print("Agora rode:")
    print("  python3 jobs/daily_update.py")

if __name__ == "__main__":
    main()
