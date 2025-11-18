import re
import pandas as pd
import os

INPUT_FILE = "boston_scientific_jobs.csv"
OUTPUT_FILE = "boston_scientific_jobs_enriched.csv"


def get_incremental_filename(base_name: str) -> str:
    """
    Return a non-conflicting filename by appending an incremental suffix
    if the base_name already exists.

    Example:
      base_name = "boston_scientific_jobs_enriched.csv"
      -> "boston_scientific_jobs_enriched.csv"  (if not exists)
      -> "boston_scientific_jobs_enriched_1.csv" (if exists)
      -> "boston_scientific_jobs_enriched_2.csv" (if both exist)
    """
    root, ext = os.path.splitext(base_name)
    candidate = base_name
    counter = 1
    while os.path.exists(candidate):
        candidate = f"{root}_{counter}{ext}"
        counter += 1
    return candidate

def split_location(location: str):
    """Split location into city, country and is_us flag."""
    if not isinstance(location, str) or not location.strip():
        return None, None, None

    # Use only the first part if there are multiple locations separated by "|"
    primary = location.split("|")[0].strip()

    # Example formats:
    # "Chicago, IL, US"
    # "Valencia US-CA, United States"
    # "Penang,MY"
    # "SG"
    parts = [p.strip() for p in primary.split(",") if p.strip()]

    city = None
    country_token = None

    if len(parts) == 1:
        # Something like "SG"
        country_token = parts[0]
    elif len(parts) >= 2:
        city = parts[0]
        country_token = parts[-1]

    code_map = {
        "US": "United States",
        "United States": "United States",
        "USA": "United States",
        "NL": "Netherlands",
        "Netherlands": "Netherlands",
        "FR": "France",
        "France": "France",
        "IE": "Ireland",
        "IRL": "Ireland",
        "MY": "Malaysia",
        "SG": "Singapore",
        "CR": "Costa Rica",
    }

    country = None
    if country_token:
        country = code_map.get(country_token, country_token)

    is_us = False
    if country:
        lowered = country.strip().lower()
        is_us = lowered in ("united states", "us", "usa", "u.s.")

    return city, country, is_us


def parse_published(published: str):
    """
    Converte frasi tipo:
      - 'Pubblicato 5 giorni fa'
      - 'Pubblicato un giorno fa'
      - "Pubblicato un'ora fa"
      - 'Pubblicato 9 minuti fa'
      - 'Pubblicato un mese fa'
      - 'Pubblicato 2 mesi fa'
    in numero di giorni (float).
    """
    if not isinstance(published, str) or not published.strip():
        return None

    txt = published.strip().lower()

    # Togli 'pubblicato' e 'fa' se presenti, per semplificare
    if txt.startswith("pubblicato"):
        txt = txt[len("pubblicato"):].strip()
    if txt.endswith(" fa"):
        txt = txt[:-3].strip()

    # Gestione extra eventuali (non presenti ora ma utili)
    if txt in {"oggi"}:
        return 0.0
    if txt in {"ieri"}:
        return 1.0

    # Numero: prima cerca cifra, altrimenti gestisci 'un', "un'", 'una'
    m_num = re.search(r"(\d+)", txt)
    if m_num:
        num = float(m_num.group(1))
    else:
        # 'un giorno fa', "un'ora fa", 'una settimana fa', 'un mese fa'
        if re.search(r"\bun\b", txt) or "un'" in txt or re.search(r"\buna\b", txt):
            num = 1.0
        else:
            # pattern sconosciuto
            return None

    # Unità di tempo
    # attenzione all'ordine: prima minuti/ore, poi giorni/mesi/anni
    if "minut" in txt:          # minuti / minuto
        factor = 1.0 / 1440.0   # 60*24
    elif "ora" in txt or "ore" in txt:  # ora / ore / un'ora
        factor = 1.0 / 24.0
    elif "giorn" in txt:        # giorno / giorni
        factor = 1.0
    elif "settim" in txt:       # settimana / settimane (se mai comparisse)
        factor = 7.0
    elif "mes" in txt:          # mese / mesi
        factor = 30.0
    elif "ann" in txt:          # anno / anni
        factor = 365.0
    else:
        # fallback: consideralo in giorni
        factor = 1.0

    return num * factor


def main():
    df = pd.read_csv(INPUT_FILE)

    # --- 1) Location -> city, country, is_us ---
    df[["location_city", "location_country", "location_is_us"]] = df["location"].apply(
        lambda x: pd.Series(split_location(x))
    )

    # --- 2) Published -> days since published ---
    df["published_days"] = df["published"].apply(parse_published)

    # --- 3) detail_description_text -> Python / Algorithm flags ---


    # ============================================================
    # Keyword-based flags on 'detail_description_text'
    # ============================================================

    # Work on a safe text series (empty string for NaN)
    desc = df["detail_description_text"].fillna("")

    desc = df["detail_description_text"].fillna("")
    df["has_python"] = desc.str.contains(r"\bpython\b", case=False, regex=True)
    # allow "algorithm" and "algorithms"
    df["has_algorithm"] = desc.str.contains(r"algorithm", case=False, regex=True)

    # -----------------------------
    # Algorithm / technical intensity
    # -----------------------------

    # has_optimization:
    #   matches: optimization, optimise, optimize, optimized, optimising, etc.
    df["has_optimization"] = desc.str.contains(
        r"\boptimi[sz]\w*", case=False, regex=True, na=False
    )

    # has_operations_research:
    #   matches explicit operations/operational research phrasing
    df["has_operations_research"] = desc.str.contains(
        r"\boperations research\b|\boperational research\b",
        case=False,
        regex=True,
        na=False,
    )

    # has_simulation:
    #   matches: simulation, simulations, simulate, simulated, simulating, etc.
    df["has_simulation"] = desc.str.contains(
        r"\bsimulat\w*", case=False, regex=True, na=False
    )

    # has_ml_ai:
    #   matches: machine learning, deep learning, ML, AI, artificial intelligence, data science
    df["has_ml_ai"] = desc.str_contains = desc.str.contains(
        r"\bmachine learning\b|"
        r"\bdeep learning\b|"
        r"\bML\b|"
        r"\bAI\b|"
        r"\bartificial intelligence\b|"
        r"\bdata science\b",
        case=False,
        regex=True,
        na=False,
    )

    # has_quant_stats:
    #   matches: quantitative, statistics, statistical, statistician, probability,
    #            regression, stochastic, time series, etc.
    df["has_quant_stats"] = desc.str.contains(
        r"\bquantitative\b|"
        r"\bstatistics?\b|"
        r"\bstatistical\b|"
        r"\bstatistician\b|"
        r"\bprobabilit[y|à]\b|"
        r"\bregression\b|"
        r"\bstochastic\b|"
        r"\btime series\b",
        case=False,
        regex=True,
        na=False,
    )

    # -----------------------------
    # Strategy-related flags
    # -----------------------------

    # has_strategy:
    #   matches generic strategy / strategic wording
    df["has_strategy"] = desc.str.contains(
        r"\bstrategy\b|\bstrategic\b", case=False, regex=True, na=False
    )

    # has_business_strategy:
    #   matches business/corporate/growth/product strategy, go-to-market, GTM
    df["has_business_strategy"] = desc.str.contains(
        r"\bbusiness strategy\b|"
        r"\bcorporate strategy\b|"
        r"\bgrowth strategy\b|"
        r"\bproduct strategy\b|"
        r"\bgo[- ]to[- ]market\b|"
        r"\bGTM\b",
        case=False,
        regex=True,
        na=False,
    )

    # has_roadmap:
    #   matches explicit mention of roadmaps
    df["has_roadmap"] = desc.str.contains(
        r"\broadmap\b", case=False, regex=True, na=False
    )

    # has_competitive_analysis:
    #   matches: competitive analysis, competitor analysis, market analysis, market research
    df["has_competitive_analysis"] = desc.str.contains(
        r"\bcompetitive analysis\b|"
        r"\bcompetitor analysis\b|"
        r"\bmarket analysis\b|"
        r"\bmarket research\b",
        case=False,
        regex=True,
        na=False,
    )

    # -----------------------------
    # Project / program management flags
    # -----------------------------

    # has_project_management:
    #   matches: project management, project manager, project governance
    df["has_project_management"] = desc.str.contains(
        r"\bproject management\b|"
        r"\bproject manager\b|"
        r"\bproject governance\b",
        case=False,
        regex=True,
        na=False,
    )

    # has_program_management:
    #   matches: program/programme management, program/programme manager, PMO
    df["has_program_management"] = desc.str.contains(
        r"\bprogram management\b|"
        r"\bprogramme management\b|"
        r"\bprogram manager\b|"
        r"\bprogramme manager\b|"
        r"\bPMO\b",
        case=False,
        regex=True,
        na=False,
    )

    # has_agile_scrum:
    #   matches: agile, scrum, kanban, sprint, backlog
    df["has_agile_scrum"] = desc.str.contains(
        r"\bagile\b|"
        r"\bscrum\b|"
        r"\bkanban\b|"
        r"\bsprint\b|"
        r"\bbacklog\b",
        case=False,
        regex=True,
        na=False,
    )

    # has_stakeholder_management:
    #   matches: stakeholder management, stakeholder engagement
    df["has_stakeholder_management"] = desc.str.contains(
        r"\bstakeholder management\b|"
        r"\bstakeholder engagement\b",
        case=False,
        regex=True,
        na=False,
    )

    # ============================================================
    # Aggregated flags (high-level classification)
    # ============================================================

    # is_algo_heavy:
    #   True if any of the "hard algorithms / technical" signals is present
    df["is_algo_heavy"] = (
            df["has_optimization"]
            | df["has_operations_research"]
            | df["has_simulation"]
            | df["has_ml_ai"]
            | df["has_python"]
            | df["has_algorithm"]
    )

    # is_strategic_role:
    #   True if any of the "strategy / positioning" signals is present
    df["is_strategic_role"] = (
            df["has_strategy"]
            | df["has_business_strategy"]
            | df["has_roadmap"]
            | df["has_competitive_analysis"]
    )

    # is_project_or_program_mgmt:
    #   True if any of the "project / program management" signals is present
    df["is_project_or_program_mgmt"] = (
            df["has_project_management"]
            | df["has_program_management"]
            | df["has_agile_scrum"]
            | df["has_stakeholder_management"]
    )

    # --- 4) Reorder columns ---
    desired_order = [
        "title",
        "location",
        "location_city",
        "location_country",
        "location_is_us",
        "department",
        "detail_max_salary",
        "detail_min_salary",
        "detail_work_mode",
        "job_url",
        "detail_description_html",
        "detail_description_text",
        "has_python",
        "has_algorithm",
        "detail_requisition_id",
        "insights_prev_companies",
        "insights_prev_roles",
        "insights_top_skills",
        "job_id",
        "published",
        "published_days",
        "tags",
        "detail_position_location",
    ]

    existing_order = [c for c in desired_order if c in df.columns]
    remaining_cols = [c for c in df.columns if c not in existing_order]
    df = df[existing_order + remaining_cols]

    # --- 5) Make column names nicer to read ---
    df.rename(columns=lambda c: c.replace("_", " ").title(), inplace=True)

    base_output = "boston_scientific_jobs_enriched.csv"
    output_path = get_incremental_filename(base_output)
    df.to_csv(output_path, index=False)
    print(f"Saved enriched file to: {output_path}")


if __name__ == "__main__":
    main()
