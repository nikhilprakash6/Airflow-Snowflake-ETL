from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Optional, Sequence

from airflow.hooks.base import BaseHook

import snowflake.connector


MASTER_TABLE = "MS_ADWBI.CTRL.ADWBI_EXECUTE_PROCEDURES_MASTER_TABLE"
AUDIT_TABLE = "MS_ADWBI.CTRL.ADWBI_AUDIT_ERROR_LOG_TABLE"
RUNID_SEQ = "MS_ADWBI.CTRL.ADWBI_AUDIT_ERROR_LOG_TABLE_SEQ"


@dataclass(frozen=True)
class StepRow:
    project_name: str
    job_code: str
    job_name: str
    step_number: int
    step_desc: str
    etl_logic: Optional[str]
    src_db: Optional[str]
    src_schema: Optional[str]
    src_table: Optional[str]
    tgt_db: Optional[str]
    tgt_schema: Optional[str]
    tgt_table: Optional[str]


def _get_snowflake_connection(conn_id: str):
    """
    Build a Snowflake connection from an Airflow Connection record.
    We expect the Snowflake account / role / warehouse / db / schema in 'Extra' JSON or fields.
    """
    c = BaseHook.get_connection(conn_id)
    extra = c.extra_dejson or {}

    account = extra.get("account") or c.host
    if not account:
        raise ValueError(
            f"Airflow connection '{conn_id}' is missing Snowflake 'account'. "
            f"Set it in Extra as {{\"account\":\"...\"}} or in the Host field."
        )

    return snowflake.connector.connect(
        user=c.login,
        password=c.password,
        account=account,
        warehouse=extra.get("warehouse"),
        database=extra.get("database"),
        schema=extra.get("schema"),
        role=extra.get("role"),
        autocommit=True,
    )


def _fetch_one_int(cur, sql: str, params: Sequence[Any] = ()) -> int:
    cur.execute(sql, params)
    row = cur.fetchone()
    if not row or row[0] is None:
        raise RuntimeError("Expected one integer result but got none.")
    return int(row[0])


def _fetch_steps(cur, job_code: str) -> list[StepRow]:
    cur.execute(
        f"""
        SELECT
            PROJECT_NAME,
            JOB_CODE,
            JOB_NAME,
            STEP_NUMBER,
            STEP_DESC,
            TO_VARCHAR(ETL_LOGIC) AS ETL_LOGIC_TXT,
            SOURCE_DATABASE_NAME,
            SOURCE_SCHEMA_NAME,
            SOURCE_TABLE_NAME,
            TARGET_DATABASE_NAME,
            TARGET_SCHEMA_NAME,
            TARGET_TABLE_NAME
        FROM {MASTER_TABLE}
        WHERE JOB_CODE = %s
        ORDER BY STEP_NUMBER
        """,
        (job_code,),
    )
    rows = cur.fetchall() or []
    steps: list[StepRow] = []
    for r in rows:
        steps.append(
            StepRow(
                project_name=r[0],
                job_code=r[1],
                job_name=r[2],
                step_number=int(r[3]),
                step_desc=r[4],
                etl_logic=r[5],
                src_db=r[6],
                src_schema=r[7],
                src_table=r[8],
                tgt_db=r[9],
                tgt_schema=r[10],
                tgt_table=r[11],
            )
        )
    return steps


def _audit_insert_started(cur, run_id: int, step: StepRow) -> None:
    cur.execute(
        f"""
        INSERT INTO {AUDIT_TABLE}
        (
            RUN_ID,
            PROJECT_NAME,
            JOB_CODE,
            JOB_NAME,
            STEP_NUMBER,
            STEP_DESC,
            SOURCE_DATABASE_NAME,
            SOURCE_SCHEMA_NAME,
            SOURCE_TABLE_NAME,
            TARGET_DATABASE_NAME,
            TARGET_SCHEMA_NAME,
            TARGET_TABLE_NAME,
            RUN_START_DATE,
            RUN_END_DATE,
            LOAD_STATUS,
            ERROR_MESSAGE
        )
        VALUES
        (
            %s,%s,%s,%s,%s,%s,
            %s,%s,%s,
            %s,%s,%s,
            CURRENT_TIMESTAMP(), NULL, 'STARTED', NULL
        )
        """,
        (
            run_id,
            step.project_name,
            step.job_code,
            step.job_name,
            step.step_number,
            step.step_desc,
            step.src_db,
            step.src_schema,
            step.src_table,
            step.tgt_db,
            step.tgt_schema,
            step.tgt_table,
        ),
    )


def _audit_update(cur, run_id: int, step: StepRow, status: str, error_message: Optional[str]) -> None:
    cur.execute(
        f"""
        UPDATE {AUDIT_TABLE}
        SET
            RUN_END_DATE = CURRENT_TIMESTAMP(),
            LOAD_STATUS = %s,
            ERROR_MESSAGE = %s
        WHERE
            RUN_ID = %s
            AND JOB_CODE = %s
            AND STEP_NUMBER = %s
        """,
        (status, error_message, run_id, step.job_code, step.step_number),
    )


def _execute_sql_block(conn, cur, sql_text: str) -> None:
    """
    Execute a SQL block that may contain multiple statements.

    Preferred path (newer Snowflake connector):
    - cursor.execute(sql_text, multi_statement_count=0)

    Fallback path:
    - conn.execute_string(sql_text)  (does statement splitting internally)

    Note: If you want *maximum safety* long-term, the best practice is:
    - store one statement per row in the master table (or add STATEMENT_ORDER rows)
    """
    sql_text = sql_text.strip()
    if not sql_text:
        return

    try:
        cur.execute(sql_text, multi_statement_count=0)
        return
    except TypeError:
        pass

    conn.execute_string(sql_text)


def run_etl_job(job_code: str, snowflake_conn_id: str = "snowflake_adwbi") -> str:
    """
    Airflow task entrypoint.

    What it does:
    - validates job exists in MASTER_TABLE
    - gets RUN_ID from sequence
    - reads all steps ordered by STEP_NUMBER
    - writes STARTED audit record per step
    - executes ETL_LOGIC (supports multi-statement blocks)
    - writes SUCCESS/FAILED audit record per step
    - raises on failure so Airflow marks task failed

    Batch timestamp:
    - Python generates one BATCH_TS per job run and injects it into SQL using $BATCH_TS token replacement
    """
    if not job_code or not job_code.strip():
        raise ValueError("job_code is required")

    job_code = job_code.strip()
    batch_ts = datetime.now(timezone.utc).replace(microsecond=0)
    batch_ts_literal = batch_ts.isoformat(sep=" ")

    conn = _get_snowflake_connection(snowflake_conn_id)
    cur = conn.cursor()

    run_id: Optional[int] = None
    last_sql: Optional[str] = None

    try:
        # 1) Validate job exists
        steps_found = _fetch_one_int(
            cur,
            f"SELECT COUNT(*) FROM {MASTER_TABLE} WHERE JOB_CODE = %s",
            (job_code,),
        )
        if steps_found == 0:
            raise RuntimeError(f"No steps found for JOB_CODE={job_code}")

        # 2) Create RUN_ID
        run_id = _fetch_one_int(cur, f"SELECT {RUNID_SEQ}.NEXTVAL")

        # 3) Fetch ordered steps
        steps = _fetch_steps(cur, job_code)

        # 4) Execute each step
        for step in steps:
            _audit_insert_started(cur, run_id, step)

            try:
                if step.etl_logic and step.etl_logic.strip():
                    sql_text = step.etl_logic.replace("$BATCH_TS", f"'{batch_ts_literal}'")
                    last_sql = sql_text

                    _execute_sql_block(conn, cur, sql_text)

                _audit_update(cur, run_id, step, status="SUCCESS", error_message=None)

            except Exception as e:
                err = str(e)
                _audit_update(cur, run_id, step, status="FAILED", error_message=err)

                raise RuntimeError(
                    f"JOB FAILED | JOB={job_code} | RUN_ID={run_id} | STEP={step.step_number} "
                    f"| DESC={step.step_desc} | ERROR={err}"
                ) from e

        return f"SUCCESS: JOB_CODE={job_code}, RUN_ID={run_id}, BATCH_TS={batch_ts.isoformat()}"

    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass

