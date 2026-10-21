from pathlib import Path

from ..config import REGIONS, S3Settings
from ..infrastructure.duckdb import SQL, export_query, vacancies_connection
from ..infrastructure.files import write_json


def export_salary_tables(connection, output: Path, min_support: int) -> None:
    highlighted = []
    for size in (2, 3):
        for direction, order, sign in (("increase", "DESC", ">0"), ("decrease", "ASC", "<0")):
            rows = export_query(
                connection,
                f"""
                SELECT '{direction}' AS direction,* FROM salary_effects
                WHERE k={size} AND salary_n>={min_support} AND base_salary_n>={min_support}
                  AND delta_rub{sign}
                ORDER BY delta_rub {order},salary_n DESC,stack LIMIT 10
            """,
                output / f"salary_k{size}_{direction}.csv",
            )
            highlighted.extend((row["k"], row["stack"]) for row in rows)

    connection.execute("CREATE TEMP TABLE highlighted(k INTEGER, stack VARCHAR[])")
    if highlighted:
        connection.executemany("INSERT INTO highlighted VALUES (?,?)", highlighted)
    for dimension in ("experience", "area"):
        filename = (
            "salary_experience_context.csv"
            if dimension == "experience"
            else "salary_regions_context.csv"
        )
        export_query(
            connection,
            f"""
            SELECT c.k,c.stack,c.{dimension},count(*) AS vacancies,
                   count(c.salary_net) AS salary_n,median(c.salary_net) AS median_net_rub
            FROM combo_rows c JOIN highlighted h ON c.k=h.k AND c.stack=h.stack
            GROUP BY c.k,c.stack,c.{dimension}
            ORDER BY c.k,c.stack,c.{dimension}
        """,
            output / filename,
        )


def export_region_tables(connection, output: Path) -> None:
    export_query(
        connection,
        """
        SELECT area,count(*) AS vacancies,count(*) FILTER(WHERE len(skills)>0) AS with_skills,
               count(salary_net) AS salary_n,100*count(salary_net)/count(*) AS net_salary_coverage_pct,
               median(salary_net) AS median_net_rub,
               quantile_cont(salary_net,0.25) AS p25_net_rub,
               quantile_cont(salary_net,0.75) AS p75_net_rub,
               count(salary_gross) AS gross_n, median(salary_gross) AS median_gross_rub,
               avg(len(skills)) AS mean_skills,
               count(*) FILTER(WHERE schedule='Удаленная работа') AS remote_vacancies,
               100*count(*) FILTER(WHERE schedule='Удаленная работа')/count(*) AS remote_pct
        FROM analysis_v WHERE area IN (?,?,?) GROUP BY area ORDER BY vacancies DESC
    """,
        output / "regions_summary.csv",
        list(REGIONS),
    )
    export_query(
        connection,
        """
        WITH stats AS (
            SELECT area,k,stack,count(*) AS frequency,count(salary_net) AS salary_n,
                   median(salary_net) AS median_net_rub
            FROM combo_rows WHERE area IN (?,?,?) GROUP BY area,k,stack
        ), ranked AS (
            SELECT *,row_number() OVER(PARTITION BY area,k ORDER BY frequency DESC,stack) AS rank
            FROM stats
        ) SELECT r.*,v.total_vacancies,100*r.frequency/v.total_vacancies AS share_pct
          FROM ranked r JOIN (SELECT area,count(*) AS total_vacancies FROM analysis_v GROUP BY area) v USING(area)
          WHERE rank<=10 ORDER BY area,k,rank
    """,
        output / "regions_top_skills.csv",
        list(REGIONS),
    )
    export_query(
        connection,
        """
        SELECT area,k,stack,count(*) AS frequency,count(salary_net) AS salary_n,
               median(salary_net) AS median_net_rub
        FROM combo_rows WHERE area IN (?,?,?)
        GROUP BY area,k,stack ORDER BY area,k,frequency DESC,stack
    """,
        output / "regions_all_combinations.csv",
        list(REGIONS),
    )
    export_query(
        connection,
        """
        SELECT area,experience,count(*) AS vacancies,count(salary_net) AS salary_n,
               median(salary_net) AS median_net_rub FROM analysis_v
        WHERE area IN (?,?,?) GROUP BY area,experience ORDER BY area,experience
    """,
        output / "regions_experience.csv",
        list(REGIONS),
    )


def export_coverage_and_growth(connection, output: Path) -> None:
    export_query(
        connection,
        """
        SELECT currency,gross,count(*) AS vacancies,
               count(*) FILTER(WHERE salary_from IS NOT NULL OR salary_to IS NOT NULL) AS with_salary,
               count(salary_net) AS accepted_net,count(salary_gross) AS accepted_gross
        FROM analysis_v GROUP BY currency,gross ORDER BY vacancies DESC
    """,
        output / "salary_coverage.csv",
    )
    export_query(
        connection,
        """
        SELECT len(skills) AS stack_size,count(*) AS vacancies,sum(len(skills)) AS emitted_k1,
               sum(len(skills)*(len(skills)-1)/2)::BIGINT AS emitted_k2,
               sum(len(skills)*(len(skills)-1)*(len(skills)-2)/6)::BIGINT AS emitted_k3
        FROM vacancies GROUP BY stack_size ORDER BY stack_size
    """,
        output / "stack_growth.csv",
    )


def analyze_vacancies(settings: S3Settings, output: Path, min_support: int = 20) -> dict:
    if min_support < 1:
        raise ValueError("Minimum salary support must be positive")
    with vacancies_connection(settings, analysis=True) as connection:
        connection.execute((SQL / "analysis.sql").read_text())
        export_salary_tables(connection, output, min_support)
        export_region_tables(connection, output)
        export_coverage_and_growth(connection, output)
        vacancies, net_count, gross_count, median = connection.execute(
            "SELECT count(*),count(salary_net),count(salary_gross),median(salary_net) FROM analysis_v"
        ).fetchone()
    result = {
        "vacancies": vacancies,
        "net_salary_n": net_count,
        "gross_salary_n": gross_count,
        "median_net_rub": median,
        "regions": REGIONS,
        "min_salary_support": min_support,
        "salary_policy": "RUR/RUB; gross=false; midpoint or known endpoint; positive consistent bounds",
        "pair_baseline": "most frequent constituent single skill in all Java vacancies",
        "triple_baseline": "most frequent constituent pair in all Java vacancies",
        "interpretation": "descriptive association, not a causal effect",
    }
    write_json(output / "analysis.json", result)
    return result
