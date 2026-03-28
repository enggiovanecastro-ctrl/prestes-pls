"""
excel_to_json.py
Converte BD_Dados_Consolidado_PLS R$.xlsx → data/pls_data.json
Executado pelo GitHub Actions a cada push do Excel.
"""

import pandas as pd
import json
from pathlib import Path
from datetime import datetime

EXCEL_PATH = Path("data/PLS.xlsx")
OUTPUT_PATH = Path("data/pls_data.json")

ANO_INI = 2022
ANO_FIM = 2030


def fmt_snap(ts) -> str:
    return ts.strftime("%Y-%m")


def load_excel() -> pd.DataFrame:
    df = pd.read_excel(EXCEL_PATH, header=7, sheet_name=0)
    df = df.dropna(subset=["Empreendimento"])
    df = df[df["Empreendimento"].astype(str).str.strip() != ""]
    return df


def get_date_cols(df: pd.DataFrame) -> list:
    return [c for c in df.columns if hasattr(c, "year") and ANO_INI <= c.year <= ANO_FIM]


def get_snaps_com_dados(df: pd.DataFrame) -> list:
    snap_totals = df.groupby("Mês")["Total Tendência"].sum()
    return sorted(snap_totals[snap_totals > 0].index)


def get_snap_atual(df: pd.DataFrame):
    return get_snaps_com_dados(df)[-1]


def build_obras(df: pd.DataFrame) -> list:
    snap_max = get_snap_atual(df)
    snap_df = df[df["Mês"] == snap_max].copy()
    date_cols = get_date_cols(df)
    anos = list(range(ANO_INI, ANO_FIM + 1))

    obras = []
    seen = set()
    for _, row in snap_df.iterrows():
        emp = str(row["Empreendimento"]).strip()
        if emp in seen or emp == "nan":
            continue
        seen.add(emp)

        anos_vals = {}
        for ano in anos:
            cols_ano = [c for c in date_cols if c.year == ano]
            total = sum(row[c] for c in cols_ano if pd.notna(row[c]))
            anos_vals[str(ano)] = round(total)

        obras.append({
            "e": emp,
            "cod": int(row["Cod"]) if pd.notna(row.get("Cod")) else None,
            "t": round(float(row["Total Tendência"])) if pd.notna(row["Total Tendência"]) else 0,
            "r": round(float(row["Realizado"])) if pd.notna(row["Realizado"]) else 0,
            "a": round(float(row["A Realizar"])) if pd.notna(row["A Realizar"]) else 0,
            "s": fmt_snap(snap_max),
            "anos": anos_vals,
        })

    obras.sort(key=lambda x: x["t"], reverse=True)
    return obras


def build_mensal_consolidado(df: pd.DataFrame) -> dict:
    """
    Para cada mês-competência usa o snapshot mais recente.
    Meses históricos ausentes no snap atual são buscados
    nos snapshots correspondentes ao período.
    """
    date_cols = get_date_cols(df)
    snaps = get_snaps_com_dados(df)
    snap_max = snaps[-1]
    snap_max_df = df[df["Mês"] == snap_max]

    mensal = {}
    for col in date_cols:
        total = snap_max_df[col].fillna(0).sum()
        if total == 0:
            # Buscar no snap mais recente que cobre esse mês
            col_naive = col if not hasattr(col, 'tzinfo') else col.replace(tzinfo=None)
            candidatos = [s for s in snaps if s.to_pydatetime().replace(tzinfo=None) <= col_naive]
            for snap in reversed(candidatos):
                snap_df = df[df["Mês"] == snap]
                total = snap_df[col].fillna(0).sum()
                if total > 0:
                    break
        if total != 0:
            mensal[col.strftime("%Y-%m")] = round(total)

    return mensal


def build_snapshots(df: pd.DataFrame) -> list:
    return [fmt_snap(s) for s in get_snaps_com_dados(df)]


def build_snapshot_data(df: pd.DataFrame) -> dict:
    date_cols = get_date_cols(df)
    result = {}
    for snap in get_snaps_com_dados(df):
        snap_df = df[df["Mês"] == snap]
        mensal = {}
        for col in date_cols:
            total = snap_df[col].fillna(0).sum()
            if total != 0:
                mensal[col.strftime("%Y-%m")] = round(total)
        result[fmt_snap(snap)] = mensal
    return result


def build_bp_data(df: pd.DataFrame) -> dict:
    date_cols = get_date_cols(df)
    result = {}
    for snap in get_snaps_com_dados(df):
        snap_key = fmt_snap(snap)
        snap_df = df[df["Mês"] == snap]
        obras_snap = {}
        seen = set()
        for _, row in snap_df.iterrows():
            emp = str(row["Empreendimento"]).strip()
            if emp in seen or emp == "nan":
                continue
            seen.add(emp)
            mensal = {}
            for col in date_cols:
                v = row[col]
                if pd.notna(v) and v != 0:
                    mensal[col.strftime("%Y-%m")] = round(float(v))
            if mensal:
                obras_snap[emp] = mensal
        result[snap_key] = obras_snap
    return result


def build_data_array(df: pd.DataFrame) -> list:
    """
    Reconstrói o array DATA[] do dash original:
    cada item = {e, s, l, r, a, t, c:{mes:valor,...}}
    Usado pelas abas Dashboard PLS e BP vs Tendência.
    """
    date_cols = get_date_cols(df)
    snaps = get_snaps_com_dados(df)
    MES_PT = ['jan','fev','mar','abr','mai','jun','jul','ago','set','out','nov','dez']

    result = []
    for snap in snaps:
        snap_str = snap.strftime("%Y-%m")
        label = f"{MES_PT[snap.month - 1]}/{str(snap.year)[2:]}"
        snap_df = df[df["Mês"] == snap]
        seen = set()

        for _, row in snap_df.iterrows():
            emp = str(row["Empreendimento"]).strip()
            if emp in seen or emp == "nan":
                continue
            seen.add(emp)

            t = float(row["Total Tendência"]) if pd.notna(row["Total Tendência"]) else 0
            r = float(row["Realizado"]) if pd.notna(row["Realizado"]) else 0
            a = float(row["A Realizar"]) if pd.notna(row["A Realizar"]) else 0

            c = {}
            for col in date_cols:
                v = row[col]
                if pd.notna(v) and v != 0:
                    c[col.strftime("%Y-%m")] = round(float(v))

            result.append({"e": emp, "s": snap_str, "l": label,
                           "r": round(r), "a": round(a), "t": round(t), "c": c})
    return result


def main():
    print(f"[{datetime.now():%H:%M:%S}] Lendo {EXCEL_PATH}...")
    df = load_excel()
    snaps = get_snaps_com_dados(df)
    snap_max = snaps[-1]
    print(f"  → {len(df)} linhas | {len(snaps)} snapshots com dados | {df['Empreendimento'].nunique()} empreendimentos")

    print("  → Construindo portfólio...")
    obras = build_obras(df)

    print("  → Construindo fluxo mensal consolidado...")
    mensal = build_mensal_consolidado(df)

    print("  → Construindo dados por snapshot...")
    snap_data = build_snapshot_data(df)

    print("  → Construindo dados BP por obra...")
    bp_data = build_bp_data(df)

    print("  → Construindo array DATA (dashboard)...")
    data_array = build_data_array(df)

    payload = {
        "meta": {
            "gerado_em": datetime.now().isoformat(),
            "snapshot_atual": fmt_snap(snap_max),
            "total_snapshots": len(snaps),
            "total_obras": len(obras),
        },
        "PORT": {
            "obras": obras,
            "mensal": mensal,
            "todos_snaps": [fmt_snap(s) for s in snaps],
        },
        "SNAP_DATA": snap_data,
        "BP_DATA": bp_data,
        "DATA": data_array,
    }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))

    size_kb = OUTPUT_PATH.stat().st_size / 1024
    print(f"[OK] {OUTPUT_PATH} gerado — {size_kb:.0f} KB")
    print(f"     Snapshot atual: {fmt_snap(snap_max)} | Obras: {len(obras)} | Snapshots: {len(snaps)} | Meses: {len(mensal)}")


if __name__ == "__main__":
    main()
