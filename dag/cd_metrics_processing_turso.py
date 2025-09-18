import dagster as dg
import pandas as pd
import traceback
import datetime

from .cd_constants import EXCHANGES, get_today
from .resources import TursoResource # Changed from PostgresResource


# Helper function to convert cursor output to a list of dictionaries
def _cursor_to_list_of_dicts(cursor):
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]

# Helper function to safely quote values for SQL queries
def sql_quote(value):
    if value is None:
        return "NULL"
    elif isinstance(value, (int, float)):
        return str(value)
    elif isinstance(value, bool):
        return str(int(value)) # Convert bool to 0 or 1 for SQLite
    elif isinstance(value, (datetime.date, datetime.datetime)):
        return f"'{value.strftime('%Y-%m-%d')}'"
    else:
        # Escape single quotes for SQL strings
        return f"'{str(value).replace("'", "''")}'"


@dg.asset(
    kinds={"turso"},
    group_name="CD_TURSO",
    tags={"data_tier": "gold", "domain": "finance", "source": "pykrx"},
    deps=["cd_update_price_marketcap_bppedd_to_security_turso"], # Dependency name updated
)
def cd_update_marketcap_from_security_to_company_turso( # Renamed
    context: dg.AssetExecutionContext,
    cd_turso: TursoResource, # Changed
) -> dg.MaterializeResult:
    """
    security의 marketcap, marketcapDate를 company에 연결 (한국 시장 대상) - TursoDB 버전.
    데드락 방지를 위해 단일 트랜잭션으로 처리합니다.
    """
    columns = ["date", "company_id", "name", "count", "marketcap"]
    updated_company_data_for_metadata = []
    max_date_str = None

    with cd_turso.get_connection() as conn:
        cur = conn.cursor()
        try:
            # 1. 지정된 거래소(EXCHANGES) 내에서 최신 marketcap_date 조회
            exchanges_tuple = tuple(EXCHANGES)
            placeholders = ', '.join(['?'] * len(exchanges_tuple))
            max_date_query = f"SELECT MAX(marketcap_date) FROM security WHERE exchange IN ({placeholders})"
            cur.execute(max_date_query, exchanges_tuple)
            max_date_row = cur.fetchone()
            max_date_str = max_date_row[0] if max_date_row and max_date_row[0] else None

            if not max_date_str:
                context.log.info("No marketcap data found for specified exchanges")
                return dg.MaterializeResult(
                    metadata={
                        "Date": dg.MetadataValue.text(""),
                        "Result": dg.MetadataValue.text("업데이트된 데이터가 없습니다"),
                    }
                )

            # 2. 회사별 marketcap 집계 데이터 조회
            company_marketcap_query = f"""
            SELECT
                c.company_id,
                c.name,
                COUNT(s.security_id) AS security_count,
                SUM(s.marketcap) AS total_marketcap
            FROM company c
            JOIN security s ON c.company_id = s.company_id
            WHERE c.type = '상장법인' AND s.exchange IN ({placeholders}) AND s.marketcap IS NOT NULL AND s.marketcap_date = ?
            GROUP BY c.company_id, c.name
            HAVING SUM(s.marketcap) > 0
            ORDER BY c.company_id;
            """
            cur.execute(company_marketcap_query, exchanges_tuple + (max_date_str,))
            company_marketcap_data = _cursor_to_list_of_dicts(cur)

            update_statements = []
            if company_marketcap_data:
                for row in company_marketcap_data:
                    update_statements.append(
                        f"UPDATE company "
                        f"SET marketcap = {sql_quote(row['total_marketcap'])}, marketcap_date = {sql_quote(max_date_str)} "
                        f"WHERE company_id = {sql_quote(row['company_id'])};"
                    )
                    updated_company_data_for_metadata.append([max_date_str, row['company_id'], row['name'], row['security_count'], row['total_marketcap']])
            
            if update_statements:
                conn.executescript("BEGIN;\n" + "\n".join(update_statements) + "\nCOMMIT;")
                context.log.info(f"Successfully updated {len(updated_company_data_for_metadata)} companies with marketcap data for {max_date_str}")
            else:
                context.log.info(f"No companies to update with marketcap data for {max_date_str}")

        except Exception as e:
            context.log.error(f"Error updating company marketcap: {str(e)}\n{traceback.format_exc()}")
            conn.rollback() # Ensure rollback on error if executescript didn't commit
            raise
        finally:
            cur.close()

    odf = pd.DataFrame(updated_company_data_for_metadata, columns=columns)
    
    final_date_str = max_date_str if max_date_str else ""

    if not odf.empty:
        metadata = {
            "Date": dg.MetadataValue.text(final_date_str),
            "# of updating marketcap to company": dg.MetadataValue.text(str(len(odf))),
            "Preview head": dg.MetadataValue.md(odf.head().to_markdown(index=False)),
            "Preview tail": dg.MetadataValue.md(odf.tail().to_markdown(index=False)),
            "dagster/row_count": dg.MetadataValue.int(len(odf)),
        }
    else:
        metadata = {
            "Date": dg.MetadataValue.text(final_date_str),
            "Result": dg.MetadataValue.text("업데이트된 데이터가 없습니다"),
            "dagster/row_count": dg.MetadataValue.int(0),
        }

    return dg.MaterializeResult(metadata=metadata)


@dg.asset(
    kinds={"turso"},
    group_name="CD_TURSO",
    tags={"data_tier": "gold", "domain": "finance", "source": "pykrx"},
    deps=[ # Dependencies updated
        "cd_sync_price_to_security_turso",
        "cd_sync_marketcaps_to_security_turso",
        "cd_sync_bppedds_to_security_turso",
    ],
)
def cd_update_price_marketcap_bppedd_to_security_turso( # Renamed
    context: dg.AssetExecutionContext,
    cd_turso: TursoResource, # Changed
) -> dg.MaterializeResult:
    """
    "Price", "Marketcap", "Bppedd" 데이터를 이용해 "Security" 테이블과 연결합니다 (TursoDB 버전).
    각 테이블의 최신(date 기준) 데이터를 바탕으로 모든 항목을 업데이트합니다.
    """
    today_date_str = get_today() 
    processed_security_data_for_metadata = []

    with cd_turso.get_connection() as conn:
        cur = conn.cursor()
        try:
            exchanges_tuple = tuple(EXCHANGES)
            placeholders = ', '.join(['?'] * len(exchanges_tuple))
            securities_query = f"""
            SELECT security_id, exchange, ticker, name
            FROM security
            WHERE exchange IN ({placeholders}) AND delisting_date IS NULL
            ORDER BY security_id;
            """
            cur.execute(securities_query, exchanges_tuple)
            securities = _cursor_to_list_of_dicts(cur)

            update_statements = []
            
            for sec_info in securities:
                sec_id = sec_info['security_id']
                
                # Price
                cur.execute("SELECT close, date AS price_date FROM price WHERE security_id = ? ORDER BY date DESC LIMIT 1", (sec_id,))
                price_data_list = _cursor_to_list_of_dicts(cur) # _cursor_to_list_of_dicts consumes cursor
                price_data = price_data_list[0] if price_data_list else {}
                
                # Marketcap
                cur.execute("SELECT shares, marketcap, date AS marketcap_date FROM marketcap WHERE security_id = ? ORDER BY date DESC LIMIT 1", (sec_id,))
                marketcap_data_list = _cursor_to_list_of_dicts(cur)
                marketcap_data = marketcap_data_list[0] if marketcap_data_list else {}
                
                # Bppedd
                cur.execute("SELECT bps, per, pbr, eps, div, dps, date AS bppedd_date FROM bppedd WHERE security_id = ? ORDER BY date DESC LIMIT 1", (sec_id,))
                bppedd_data_list = _cursor_to_list_of_dicts(cur)
                bppedd_data = bppedd_data_list[0] if bppedd_data_list else {}

                set_clauses = []
                current_sec_data_for_meta = {
                    'price': None, 'price_date': None, 'shares': None, 'shares_date': None, 
                    'marketcap': None, 'marketcap_date': None, 'bps': None, 'bps_date': None,
                    'per': None, 'per_date': None, 'pbr': None, 'pbr_date': None,
                    'eps': None, 'eps_date': None, 'div': None, 'div_date': None,
                    'dps': None, 'dps_date': None
                }

                if price_data:
                    if price_data.get('close') is not None: set_clauses.append(f"price = {sql_quote(price_data.get('close'))}")
                    current_sec_data_for_meta['price'] = price_data.get('close')
                    if price_data.get('price_date') is not None: set_clauses.append(f"price_date = {sql_quote(price_data.get('price_date'))}")
                    current_sec_data_for_meta['price_date'] = price_data.get('price_date')
                
                if marketcap_data:
                    if marketcap_data.get('shares') is not None: set_clauses.append(f"shares = {sql_quote(marketcap_data.get('shares'))}")
                    current_sec_data_for_meta['shares'] = marketcap_data.get('shares')
                    if marketcap_data.get('marketcap_date') is not None: set_clauses.append(f"shares_date = {sql_quote(marketcap_data.get('marketcap_date'))}") # shares_date from marketcap_date
                    current_sec_data_for_meta['shares_date'] = marketcap_data.get('marketcap_date')
                    if marketcap_data.get('marketcap') is not None: set_clauses.append(f"marketcap = {sql_quote(marketcap_data.get('marketcap'))}")
                    current_sec_data_for_meta['marketcap'] = marketcap_data.get('marketcap')
                    if marketcap_data.get('marketcap_date') is not None: set_clauses.append(f"marketcap_date = {sql_quote(marketcap_data.get('marketcap_date'))}")
                    current_sec_data_for_meta['marketcap_date'] = marketcap_data.get('marketcap_date')

                if bppedd_data:
                    if bppedd_data.get('bps') is not None: set_clauses.append(f"bps = {sql_quote(bppedd_data.get('bps'))}")
                    current_sec_data_for_meta['bps'] = bppedd_data.get('bps')
                    if bppedd_data.get('bppedd_date') is not None: set_clauses.append(f"bps_date = {sql_quote(bppedd_data.get('bppedd_date'))}")
                    current_sec_data_for_meta['bps_date'] = bppedd_data.get('bppedd_date')
                    # ... (repeat for per, pbr, eps, div, dps and their dates)
                    for metric in ['per', 'pbr', 'eps', 'div', 'dps']:
                        if bppedd_data.get(metric) is not None: set_clauses.append(f"{metric} = {sql_quote(bppedd_data.get(metric))}")
                        current_sec_data_for_meta[metric] = bppedd_data.get(metric)
                        if bppedd_data.get('bppedd_date') is not None: set_clauses.append(f"{metric}_date = {sql_quote(bppedd_data.get('bppedd_date'))}")
                        current_sec_data_for_meta[f'{metric}_date'] = bppedd_data.get('bppedd_date')
                
                if set_clauses:
                    update_statements.append(f"UPDATE security SET {', '.join(set_clauses)} WHERE security_id = {sql_quote(sec_id)};")

                processed_security_data_for_metadata.append([
                    today_date_str,
                    sec_info['exchange'], sec_info['security_id'], sec_info['ticker'], sec_info['name'],
                    current_sec_data_for_meta['price'], current_sec_data_for_meta['price_date'],
                    current_sec_data_for_meta['shares'], current_sec_data_for_meta['shares_date'],
                    current_sec_data_for_meta['marketcap'], current_sec_data_for_meta['marketcap_date'],
                    current_sec_data_for_meta['bps'], current_sec_data_for_meta['bps_date'],
                    current_sec_data_for_meta['per'], current_sec_data_for_meta['per_date'],
                    current_sec_data_for_meta['pbr'], current_sec_data_for_meta['pbr_date'],
                    current_sec_data_for_meta['eps'], current_sec_data_for_meta['eps_date'],
                    current_sec_data_for_meta['div'], current_sec_data_for_meta['div_date'],
                    current_sec_data_for_meta['dps'], current_sec_data_for_meta['dps_date']
                ])
            
            if update_statements:
                conn.executescript("BEGIN;\n" + "\n".join(update_statements) + "\nCOMMIT;")
                context.log.info(f"Successfully processed updates for {len(securities)} securities.")
            else:
                context.log.info("No security updates to perform.")
                
        except Exception as e:
            context.log.error(f"Error updating security data: {str(e)}\n{traceback.format_exc()}")
            conn.rollback()
            raise
        finally:
            cur.close()

    columns = [
        "date", "exchange", "security_id", "ticker", "name",
        "price", "price_date", "shares", "shares_date", "marketcap", "marketcap_date",
        "bps", "bps_date", "per", "per_date", "pbr", "pbr_date",
        "eps", "eps_date", "div", "div_date", "dps", "dps_date",
    ]
    odf = pd.DataFrame(processed_security_data_for_metadata, columns=columns)

    if not odf.empty:
        metadata = {
            "Date": dg.MetadataValue.text(today_date_str),
            "Exchange": dg.MetadataValue.text(" ".join(EXCHANGES)),
            "# of processed securities": dg.MetadataValue.text(str(len(odf))),
            "Preview head": dg.MetadataValue.md(odf.head().to_markdown(index=False)),
            "Preview middle": dg.MetadataValue.md(
                odf.iloc[len(odf) // 2 : len(odf) // 2 + 1].to_markdown(index=False) if len(odf) > 0 else ""
            ),
            "Preview tail": dg.MetadataValue.md(odf.tail().to_markdown(index=False)),
            "dagster/row_count": dg.MetadataValue.int(len(odf)),
        }
    else:
        metadata = {
            "Date": dg.MetadataValue.text(today_date_str),
            "Exchange": dg.MetadataValue.text(" ".join(EXCHANGES)),
            "Result": dg.MetadataValue.text("업데이트된 데이터가 없습니다"),
            "dagster/row_count": dg.MetadataValue.int(0),
        }

    return dg.MaterializeResult(metadata=metadata)


@dg.asset(
    kinds={"turso"},
    group_name="CD_TURSO",
    tags={"data_tier": "gold", "domain": "finance", "source": "pykrx"},
    deps=["cd_update_marketcap_from_security_to_company_turso"], # Dependency updated
)
def cd_update_rank_to_company_turso( # Renamed
    context: dg.AssetExecutionContext,
    cd_turso: TursoResource, # Changed
) -> dg.MaterializeResult:
    """
    한국 시장 상장법인(company)의 marketcap_rank를 업데이트합니다 (TursoDB 버전).
    """
    today_str = get_today()
    columns = ["date", "company_id", "name", "marketcap_rank", "marketcap_prior_rank"]
    updated_rank_data_for_metadata = []

    with cd_turso.get_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute("""
                SELECT company_id, name, marketcap, marketcap_rank AS old_rank
                FROM company
                WHERE type = '상장법인' AND marketcap IS NOT NULL
                ORDER BY company_id;
            """)
            companies_data = _cursor_to_list_of_dicts(cur)

            if not companies_data:
                context.log.info("No company data found for rank update.")
                return dg.MaterializeResult(
                    metadata={
                        "Date": dg.MetadataValue.text(today_str),
                        "Result": dg.MetadataValue.text("업데이트할 회사 데이터가 없습니다"),
                        "dagster/row_count": dg.MetadataValue.int(0),
                    }
                )
            
            df = pd.DataFrame.from_records(companies_data)
            df['marketcap'] = pd.to_numeric(df['marketcap'], errors='coerce')
            df = df.dropna(subset=['marketcap']) 
            
            if df.empty: # Check if df became empty after dropna
                context.log.info("No valid company marketcap data after cleaning for rank update.")
                return dg.MaterializeResult(
                    metadata={
                        "Date": dg.MetadataValue.text(today_str),
                        "Result": dg.MetadataValue.text("순위 업데이트를 위한 유효한 시가총액 데이터가 없습니다"),
                        "dagster/row_count": dg.MetadataValue.int(0),
                    }
                )

            df = df.sort_values(by='marketcap', ascending=False)
            df['new_rank'] = df['marketcap'].rank(method='first', ascending=False).astype(int)
            
            # Ensure old_rank is numeric for comparison, coercing errors to NaN
            df['old_rank'] = pd.to_numeric(df['old_rank'], errors='coerce')
            # Filter for actual changes: new_rank is different OR old_rank was NaN and new_rank is not
            df_filtered = df[df['old_rank'] != df['new_rank']].copy()
            
            update_statements = []
            for _, row in df_filtered.iterrows():
                update_statements.append(
                    f"UPDATE company SET marketcap_prior_rank = {sql_quote(row['old_rank'])}, "
                    f"marketcap_rank = {sql_quote(row['new_rank'])} "
                    f"WHERE company_id = {sql_quote(row['company_id'])};"
                )
                updated_rank_data_for_metadata.append([today_str, row['company_id'], row['name'], row['new_rank'], row['old_rank']])
            
            if update_statements:
                conn.executescript("BEGIN;\n" + "\n".join(update_statements) + "\nCOMMIT;")
                context.log.info(f"Successfully updated ranks for {len(updated_rank_data_for_metadata)} companies")
            else:
                context.log.info("No company ranks needed updating.")
                
        except Exception as e:
            context.log.error(f"Error updating company ranks: {str(e)}\n{traceback.format_exc()}")
            conn.rollback()
            raise
        finally:
            cur.close()

    odf = pd.DataFrame(updated_rank_data_for_metadata, columns=columns)
    if not odf.empty:
        metadata = {
            "Date": dg.MetadataValue.text(today_str),
            "# of updating rank to company": dg.MetadataValue.text(str(len(odf))),
            "Preview head": dg.MetadataValue.md(odf.head().to_markdown(index=False)),
            "Preview tail": dg.MetadataValue.md(odf.tail().to_markdown(index=False)),
            "dagster/row_count": dg.MetadataValue.int(len(odf)),
        }
    else:
        metadata = {
            "Date": dg.MetadataValue.text(today_str),
            "Result": dg.MetadataValue.text("업데이트된 데이터가 없습니다"),
            "dagster/row_count": dg.MetadataValue.int(0),
        }
    return dg.MaterializeResult(metadata=metadata)


@dg.asset(
    kinds={"turso"},
    group_name="CD_TURSO",
    tags={"data_tier": "gold", "domain": "finance", "source": "pykrx"},
    deps=["cd_update_marketcap_from_security_to_company_turso"], # Dependency updated
)
def cd_update_rank_to_security_turso( # Renamed
    context: dg.AssetExecutionContext,
    cd_turso: TursoResource, # Changed
) -> dg.MaterializeResult:
    """
    security ranking 업데이트 (한국 시장 대상) - TursoDB 버전.
    """
    today_str = get_today()
    columns = ["date", "security_id", "name", "marketcap_rank", "marketcap_prior_rank"]
    updated_rank_data_for_metadata = []

    with cd_turso.get_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute("""
                SELECT security_id, name, marketcap, marketcap_rank AS old_rank
                FROM security
                WHERE delisting_date IS NULL AND marketcap IS NOT NULL
                ORDER BY security_id;
            """)
            securities_data = _cursor_to_list_of_dicts(cur)

            if not securities_data:
                context.log.info("No security data found for rank update.")
                return dg.MaterializeResult(
                    metadata={
                        "Date": dg.MetadataValue.text(today_str),
                        "Result": dg.MetadataValue.text("업데이트할 증권 데이터가 없습니다"),
                        "dagster/row_count": dg.MetadataValue.int(0),
                    }
                )

            df = pd.DataFrame.from_records(securities_data)
            df['marketcap'] = pd.to_numeric(df['marketcap'], errors='coerce')
            df = df.dropna(subset=['marketcap'])

            if df.empty: # Check if df became empty after dropna
                context.log.info("No valid security marketcap data after cleaning for rank update.")
                return dg.MaterializeResult(
                    metadata={
                        "Date": dg.MetadataValue.text(today_str),
                        "Result": dg.MetadataValue.text("순위 업데이트를 위한 유효한 시가총액 데이터가 없습니다"),
                        "dagster/row_count": dg.MetadataValue.int(0),
                    }
                )

            df = df.sort_values(by='marketcap', ascending=False)
            df['new_rank'] = df['marketcap'].rank(method='first', ascending=False).astype(int)
            
            df['old_rank'] = pd.to_numeric(df['old_rank'], errors='coerce')
            df_filtered = df[df['old_rank'] != df['new_rank']].copy()

            update_statements = []
            for _, row in df_filtered.iterrows():
                update_statements.append(
                    f"UPDATE security SET marketcap_prior_rank = {sql_quote(row['old_rank'])}, "
                    f"marketcap_rank = {sql_quote(row['new_rank'])} "
                    f"WHERE security_id = {sql_quote(row['security_id'])};"
                )
                updated_rank_data_for_metadata.append([today_str, row['security_id'], row['name'], row['new_rank'], row['old_rank']])
            
            if update_statements:
                conn.executescript("BEGIN;\n" + "\n".join(update_statements) + "\nCOMMIT;")
                context.log.info(f"Successfully updated ranks for {len(updated_rank_data_for_metadata)} securities")
            else:
                context.log.info("No security ranks needed updating.")

        except Exception as e:
            context.log.error(f"Error updating security ranks: {str(e)}\n{traceback.format_exc()}")
            conn.rollback()
            raise
        finally:
            cur.close()

    odf = pd.DataFrame(updated_rank_data_for_metadata, columns=columns)
    if not odf.empty:
        metadata = {
            "Date": dg.MetadataValue.text(today_str),
            "# of updating rank to security": dg.MetadataValue.text(str(len(odf))),
            "Preview head": dg.MetadataValue.md(odf.head().to_markdown(index=False)),
            "Preview tail": dg.MetadataValue.md(odf.tail().to_markdown(index=False)),
            "dagster/row_count": dg.MetadataValue.int(len(odf)),
        }
    else:
        metadata = {
            "Date": dg.MetadataValue.text(today_str),
            "Result": dg.MetadataValue.text("업데이트된 데이터가 없습니다"),
            "dagster/row_count": dg.MetadataValue.int(0),
        }
    return dg.MaterializeResult(metadata=metadata)

