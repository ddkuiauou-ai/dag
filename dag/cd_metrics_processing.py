import dagster as dg
import pandas as pd
from psycopg2.extras import execute_batch

from .cd_constants import EXCHANGES, get_today
from .resources import PostgresResource


@dg.asset(
    kinds={"postgres"},
    group_name="CD",
    tags={"data_tier": "gold", "domain": "finance", "source": "pykrx"},
    deps=["cd_update_price_marketcap_bppedd_to_security"],
)
def cd_update_marketcap_from_security_to_company(
    context: dg.AssetExecutionContext,
    cd_postgres: PostgresResource,
) -> dg.MaterializeResult:
    """
    security의 marketcap, marketcapDate를 company에 연결 (한국 시장 대상).
    PostgreSQL 쿼리를 직접 사용하여 최적화하였습니다.
    데드락 방지를 위해 단일 트랜잭션으로 처리합니다.
    """
    columns = ["date", "company_id", "name", "count", "marketcap"]
    data = []

    with cd_postgres.get_connection() as conn:
        with conn.cursor() as cur:
            try:
                # 1. 지정된 거래소(EXCHANGES) 내에서 최신 marketcap_date 조회
                cur.execute(
                    'SELECT MAX(marketcap_date) FROM security WHERE exchange = ANY(%s)',
                    (EXCHANGES,),
                )
                max_date = cur.fetchone()[0]
                if not max_date:
                    context.log.info("No marketcap data found for specified exchanges")
                    return dg.MaterializeResult(
                        metadata={
                            "Date": dg.MetadataValue.text(""),
                            "Result": dg.MetadataValue.text("업데이트된 데이터가 없습니다"),
                        }
                    )

                # 2. 데드락 방지를 위한 단일 트랜잭션 업데이트
                # ORDER BY company_id로 일관된 순서 보장
                update_sql = """
                WITH company_marketcap AS (
                    SELECT
                        c.company_id,
                        c.name,
                        COUNT(s.security_id) AS security_count,
                        SUM(s.marketcap) AS total_marketcap
                    FROM company c
                    JOIN security s ON c.company_id = s.company_id
                    WHERE c.type = '상장법인' AND s.exchange = ANY(%s)
                    GROUP BY c.company_id, c.name
                    HAVING SUM(s.marketcap) > 0
                    ORDER BY c.company_id  -- 데드락 방지를 위한 일관된 순서
                )
                UPDATE company
                SET marketcap = cm.total_marketcap, 
                    marketcap_date = %s
                FROM company_marketcap cm
                WHERE company.company_id = cm.company_id
                RETURNING company.company_id, cm.name, cm.security_count, cm.total_marketcap;
                """
                
                cur.execute(update_sql, (EXCHANGES, max_date))
                updated_rows = cur.fetchall()
                
                # 업데이트된 데이터 수집
                for company_id, name, security_count, total_marketcap in updated_rows:
                    data.append([max_date, company_id, name, security_count, total_marketcap])
                
                conn.commit()
                context.log.info(f"Successfully updated {len(updated_rows)} companies with marketcap data for {max_date}")
                
            except Exception as e:
                conn.rollback()
                context.log.error(f"Error updating company marketcap: {str(e)}")
                raise

    odf = pd.DataFrame(data, columns=columns)
    date_str = (
        max_date.strftime("%Y-%m-%d")
        if hasattr(max_date, "strftime")
        else str(max_date)
    )

    if not odf.empty:
        metadata = {
            "Date": dg.MetadataValue.text(date_str),
            "# of updating marketcap to company": dg.MetadataValue.text(str(len(odf))),
            "Preview head": dg.MetadataValue.md(odf.head().to_markdown(index=False)),
            "Preview tail": dg.MetadataValue.md(odf.tail().to_markdown(index=False)),
            "dagster/row_count": dg.MetadataValue.int(len(odf)),
        }
    else:
        metadata = {
            "Date": dg.MetadataValue.text(date_str),
            "Result": dg.MetadataValue.text("업데이트된 데이터가 없습니다"),
            "dagster/row_count": dg.MetadataValue.int(0),
        }

    return dg.MaterializeResult(metadata=metadata)


@dg.asset(
    kinds={"postgres"},
    group_name="CD",
    tags={"data_tier": "gold", "domain": "finance", "source": "pykrx"},
    deps=[
        "cd_sync_price_to_security",
        "cd_sync_marketcaps_to_security",
        "cd_sync_bppedds_to_security",
    ],
)
def cd_update_price_marketcap_bppedd_to_security(
    context: dg.AssetExecutionContext,
    cd_postgres: PostgresResource,
) -> dg.MaterializeResult:
    """
    "Price", "Marketcap", "Bppedd" 데이터를 이용해 "Security" 테이블과 연결합니다.
    (한국 시장 대상)
    각 테이블의 최신(date 기준) 데이터를 바탕으로
    price, marketcap, bps, per, pbr, eps, div, dps 등 모든 항목을 업데이트합니다.
    """
    today_date = get_today()

    with cd_postgres.get_connection() as conn:
        with conn.cursor() as cur:
            try:
                # Optimized single-statement update using lateral joins and CTE
                update_sql = """
                WITH latest AS (
                    SELECT s.security_id,
                           s.exchange,
                           s.ticker,
                           s.name,
                           p.close           AS price,
                           p.date            AS price_date,
                           m.shares,
                           m.marketcap       AS marketcap,
                           m.date            AS latest_marketcap_source_date, -- Renamed to avoid confusion, this is from marketcap table
                           b.bps,
                           b.per,
                           b.pbr,
                           b.eps,
                           b.div,
                           b.dps,
                           b.date            AS bppedd_date
                    FROM security s
                    LEFT JOIN LATERAL (
                        SELECT close, date
                        FROM price
                        WHERE security_id = s.security_id
                        ORDER BY date DESC
                        LIMIT 1
                    ) p ON TRUE
                    LEFT JOIN LATERAL (
                        SELECT shares, marketcap, date
                        FROM marketcap
                        WHERE security_id = s.security_id
                        ORDER BY date DESC
                        LIMIT 1
                    ) m ON TRUE
                    LEFT JOIN LATERAL (
                        SELECT bps, per, pbr, eps, div, dps, date
                        FROM bppedd
                        WHERE security_id = s.security_id
                        ORDER BY date DESC
                        LIMIT 1
                    ) b ON TRUE
                    WHERE s.exchange = ANY(%s)
                      AND s.delisting_date IS NULL
                    ORDER BY s.security_id  -- 데드락 방지를 위한 일관된 순서
                )
                UPDATE security AS sec
                SET price             = latest.price,
                    price_date        = latest.price_date,
                    shares            = latest.shares,
                    shares_date       = latest.latest_marketcap_source_date, -- Use the date from marketcap table for shares_date
                    marketcap         = latest.marketcap,
                    marketcap_date    = latest.latest_marketcap_source_date,
                    bps               = latest.bps,
                    bps_date          = latest.bppedd_date,
                    per               = latest.per,
                    per_date          = latest.bppedd_date,
                    pbr               = latest.pbr,
                    pbr_date          = latest.bppedd_date,
                    eps               = latest.eps,
                    eps_date          = latest.bppedd_date,
                    div               = latest.div,
                    div_date          = latest.bppedd_date,
                    dps               = latest.dps,
                    dps_date          = latest.bppedd_date
                FROM latest
                WHERE sec.security_id = latest.security_id
                RETURNING sec.exchange,
                          sec.security_id,
                          sec.ticker,
                          sec.name,
                          sec.price,
                          sec.price_date,
                          sec.shares,
                          sec.shares_date,       -- This is now sourced from marketcap.date via latest.latest_marketcap_source_date
                          sec.marketcap,
                          latest.latest_marketcap_source_date, -- Return the source date for marketcap if needed for logging/DataFrame
                          sec.bps,
                          sec.bps_date,
                          sec.per,
                          sec.per_date,
                          sec.pbr,
                          sec.pbr_date,
                          sec.eps,
                          sec.eps_date,
                          sec.div,
                          sec.div_date,
                          sec.dps,
                          sec.dps_date;
                """
                cur.execute(update_sql, (EXCHANGES,))
                rows = cur.fetchall()
                conn.commit()
                context.log.info(f"Successfully updated {len(rows)} securities with latest price/marketcap/bppedd data")
                
            except Exception as e:
                conn.rollback()
                context.log.error(f"Error updating security data: {str(e)}")
                raise

    # Build 'data' list from returned rows
    # Adjust column names and order to match the new RETURNING clause
    data = []
    for row in rows:
        # Original RETURNING had 22 columns, marketcap_date was the 10th (0-indexed 9)
        # New RETURNING has 22 columns, latest_marketcap_source_date is 10th (0-indexed 9)
        data.append([today_date] + list(row))


    columns = [
        "date", # This is today_date, the execution date of the asset
        "exchange",
        "security_id",
        "ticker",
        "name",
        "price",
        "price_date",
        "shares",
        "shares_date", # This is the date from the marketcap table, associated with shares and marketcap value
        "marketcap",
        "latest_marketcap_source_date", # Date from marketcap table, for the marketcap value
        "bps",
        "bps_date",
        "per",
        "per_date",
        "pbr",
        "pbr_date",
        "eps",
        "eps_date",
        "div",
        "div_date",
        "dps",
        "dps_date",
    ]
    odf = pd.DataFrame(data, columns=columns)

    # ──── 5. 메타데이터 구성 ────
    if not odf.empty:
        metadata = {
            "Date": dg.MetadataValue.text(today_date),
            "Exchange": dg.MetadataValue.text(" ".join(EXCHANGES)),
            "# of updating marketcap to security": dg.MetadataValue.text(str(len(odf))),
            "Preview head": dg.MetadataValue.md(odf.head().to_markdown(index=False)),
            "Preview middle": dg.MetadataValue.md(
                odf.iloc[len(odf) // 2 : len(odf) // 2 + 1].to_markdown(index=False)
            ),
            "Preview tail": dg.MetadataValue.md(odf.tail().to_markdown(index=False)),
            "dagster/row_count": dg.MetadataValue.int(len(odf)),
        }
    else:
        metadata = {
            "Date": dg.MetadataValue.text(today_date),
            "Exchange": dg.MetadataValue.text(" ".join(EXCHANGES)),
            "Result": dg.MetadataValue.text("업데이트된 데이터가 없습니다"),
            "dagster/row_count": dg.MetadataValue.int(0),
        }

    return dg.MaterializeResult(metadata=metadata)


@dg.asset(
    kinds={"postgres"},
    group_name="CD",
    tags={"data_tier": "gold", "domain": "finance", "source": "pykrx"},
    deps=["cd_update_marketcap_from_security_to_company"],
)
def cd_update_rank_to_company(
    context: dg.AssetExecutionContext,
    cd_postgres: PostgresResource,
) -> dg.MaterializeResult:
    """
    한국 시장 상장법인(company)의 marketcap_rank를 업데이트합니다.
    - marketcap 기준 내림차순으로 ROW_NUMBER()를 사용하여 새 순위를 계산합니다.
    - 기존 marketcap_rank와 다를 경우에만 업데이트합니다.
    - 업데이트된 결과를 DataFrame 미리보기와 함께 metadata로 반환합니다.
    """
    today = get_today()
    columns = ["date", "company_id", "name", "marketcap_rank", "marketcap_prior_rank"]

    # PostgreSQL 연결 및 커서 사용
    with cd_postgres.get_connection() as conn:
        with conn.cursor() as cur:
            try:
                # 새 순위를 계산한 후 기존 순위와 다른 경우에만 업데이트하는 쿼리
                sql = """
                WITH ranked AS (
                    SELECT company_id,
                        ROW_NUMBER() OVER (ORDER BY marketcap DESC) AS new_rank
                    FROM company
                    WHERE type = '상장법인' AND marketcap IS NOT NULL
                    ORDER BY company_id  -- 데드락 방지를 위한 일관된 순서
                )
                UPDATE company AS comp
                SET marketcap_prior_rank = comp.marketcap_rank,
                    marketcap_rank = ranked.new_rank
                FROM ranked
                WHERE comp.company_id = ranked.company_id
                AND ranked.new_rank IS DISTINCT FROM comp.marketcap_rank
                RETURNING comp.company_id, comp.name, ranked.new_rank, comp.marketcap_rank AS old_rank;
                """
                cur.execute(sql)
                rows = cur.fetchall()
                conn.commit()
                context.log.info(f"Successfully updated ranks for {len(rows)} companies")
                
            except Exception as e:
                conn.rollback()
                context.log.error(f"Error updating company ranks: {str(e)}")
                raise

    # 업데이트된 결과가 있으면 DataFrame과 metadata 생성
    if rows:
        data = [[today, *row] for row in rows]
        odf = pd.DataFrame(data, columns=columns)
        metadata = {
            "Date": dg.MetadataValue.text(today),
            "# of updating rank to company": dg.MetadataValue.text(str(len(odf))),
            "Preview head": dg.MetadataValue.md(odf.head().to_markdown(index=False)),
            "Preview tail": dg.MetadataValue.md(odf.tail().to_markdown(index=False)),
            "dagster/row_count": dg.MetadataValue.int(len(odf)),
        }
    else:
        metadata = {
            "Date": dg.MetadataValue.text(today),
            "Result": dg.MetadataValue.text("업데이트된 데이터가 없습니다"),
            "dagster/row_count": dg.MetadataValue.int(0),
        }

    return dg.MaterializeResult(metadata=metadata)


METRICS_TO_PROCESS = [
    {'name': 'marketcap', 'column': 'marketcap', 'date_column': 'marketcap_date'},
    {'name': 'bps', 'column': 'bps', 'date_column': 'bps_date'},
    {'name': 'per', 'column': 'per', 'date_column': 'per_date'},
    {'name': 'pbr', 'column': 'pbr', 'date_column': 'pbr_date'},
    {'name': 'eps', 'column': 'eps', 'date_column': 'eps_date'},
    {'name': 'div', 'column': 'div', 'date_column': 'div_date'},
    {'name': 'dps', 'column': 'dps', 'date_column': 'dps_date'},
]
ORDER_DIRECTION = "DESC NULLS LAST"


@dg.asset(
    kinds={"postgres"},
    group_name="CD",
    tags={"data_tier": "gold", "domain": "finance", "source": "calculated"},
    deps=["cd_update_price_marketcap_bppedd_to_security"],  # Depends on security table values being up-to-date
)
def cd_populate_security_ranks(
    context: dg.AssetExecutionContext,
    cd_postgres: PostgresResource,
) -> dg.MaterializeResult:
    """
    Calculates and updates ranks for various metrics (marketcap, bps, per, etc.)
    for securities and stores them in the 'security_rank' table.
    Ranks are based on a single processing_rank_date derived from the latest
    available data in the 'security' table.
    """
    processed_metrics_summary = {}
    all_upsert_data = []
    total_updated_rows = 0

    with cd_postgres.get_connection() as conn:
        with conn.cursor() as cur:
            try:
                # 1. Determine the processing_rank_date
                # Using marketcap_date as the reference, can be adjusted
                # Ensure EXCHANGES is defined, e.g., from .cd_constants
                cur.execute(
                    f"""
                    SELECT MAX(marketcap_date) 
                    FROM security 
                    WHERE marketcap_date IS NOT NULL 
                      AND exchange = ANY(%s) 
                      AND delisting_date IS NULL;
                    """,
                    (EXCHANGES,),
                )
                result = cur.fetchone()
                if not result or not result[0]:
                    context.log.warning(
                        "Could not determine processing_rank_date from security.marketcap_date. No ranks processed."
                    )
                    return dg.MaterializeResult(
                        metadata={"Result": "No processing_rank_date found, no ranks processed."}
                    )
                processing_rank_date = result[0]
                context.log.info(f"Determined processing_rank_date: {processing_rank_date}")

                for metric_config in METRICS_TO_PROCESS:
                    metric_type = metric_config['name']
                    metric_column = metric_config['column']
                    source_metric_date_column = metric_config['date_column'] # Date column in security table
                    
                    context.log.info(f"Processing ranks for metric: {metric_type}")

                    # 2. Calculate new ranks and fetch prior ranks in one go
                    # Ensure :processing_rank_date is correctly substituted or passed as parameter
                    # The SQL query needs to be carefully constructed and parameterized
                    # For simplicity in this draft, using f-string for column names, but use %s for values.
                    
                    sql_query = f"""
                    WITH new_metric_ranks AS (
                        SELECT
                            s.security_id,
                            %s::metric_type AS metric_type, -- param: metric_type
                            %s AS rank_date,    -- param: processing_rank_date
                            s.{metric_column} AS value,
                            ROW_NUMBER() OVER (ORDER BY s.{metric_column} {ORDER_DIRECTION}) AS current_r
                        FROM security s
                        WHERE 
                            s.delisting_date IS NULL 
                            AND s.{metric_column} IS NOT NULL 
                            AND s.{source_metric_date_column} = %s -- param: processing_rank_date (ensure data is from this date)
                            AND s.exchange = ANY(%s) -- param: EXCHANGES
                    ),
                    last_prior_ranks AS (
                        SELECT
                            sr_lp.security_id,
                            sr_lp.metric_type,
                            sr_lp.current_rank AS prior_r
                        FROM security_rank sr_lp
                        INNER JOIN (
                            SELECT 
                                security_id, 
                                metric_type,
                                MAX(rank_date) AS max_prior_date
                            FROM security_rank
                            WHERE 
                                metric_type = %s::metric_type -- param: metric_type
                                AND rank_date < %s   -- param: processing_rank_date
                            GROUP BY security_id, metric_type
                        ) latest_dates ON sr_lp.security_id = latest_dates.security_id 
                                       AND sr_lp.metric_type = latest_dates.metric_type 
                                       AND sr_lp.rank_date = latest_dates.max_prior_date
                        WHERE sr_lp.metric_type = %s::metric_type -- param: metric_type (redundant but safe)
                    )
                    SELECT
                        nmr.security_id,
                        nmr.metric_type,
                        nmr.rank_date,
                        nmr.current_r,
                        lpr.prior_r,
                        nmr.value
                    FROM new_metric_ranks nmr
                    LEFT JOIN last_prior_ranks lpr ON nmr.security_id = lpr.security_id 
                                                 AND nmr.metric_type = lpr.metric_type;
                    """
                    
                    cur.execute(sql_query, (
                        metric_type, processing_rank_date, processing_rank_date, EXCHANGES,
                        metric_type, processing_rank_date, metric_type
                    ))
                    
                    metric_upsert_data = []
                    fetched_rows = cur.fetchall()
                    for row in fetched_rows:
                        # (security_id, metric_type, rank_date, current_rank, prior_rank, value)
                        metric_upsert_data.append(row)
                    
                    all_upsert_data.extend(metric_upsert_data)
                    processed_metrics_summary[metric_type] = len(metric_upsert_data)
                    context.log.info(f"Prepared {len(metric_upsert_data)} rows for metric: {metric_type}")

                if all_upsert_data:
                    upsert_sql = """
                    INSERT INTO security_rank (
                        security_id, metric_type, rank_date, current_rank, prior_rank, value
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (security_id, metric_type, rank_date) DO UPDATE SET
                        current_rank = EXCLUDED.current_rank,
                        prior_rank = EXCLUDED.prior_rank,
                        value = EXCLUDED.value,
                        updated_at = NOW();
                    """
                    execute_batch(cur, upsert_sql, all_upsert_data, page_size=500)
                    total_updated_rows = len(all_upsert_data)
                    conn.commit()
                    context.log.info(f"Successfully upserted {total_updated_rows} rows into security_rank.")
                else:
                    context.log.info("No data to upsert into security_rank.")

            except Exception as e:
                conn.rollback()
                context.log.error(f"Error populating security_rank: {str(e)}")
                raise

    metadata = {
        "processing_rank_date": dg.MetadataValue.text(str(processing_rank_date) if 'processing_rank_date' in locals() else "N/A"),
        "total_rows_upserted": dg.MetadataValue.int(total_updated_rows),
        "metrics_processed_counts": dg.MetadataValue.json(processed_metrics_summary),
    }
    if total_updated_rows > 0 and all_upsert_data:
        # Create a small preview
        preview_df = pd.DataFrame(all_upsert_data[:5], columns=["security_id", "metric_type", "rank_date", "current_rank", "prior_rank", "value"])
        metadata["preview_head"] = dg.MetadataValue.md(preview_df.to_markdown(index=False))
    
    return dg.MaterializeResult(metadata=metadata)

