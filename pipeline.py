#!/usr/bin/env python
"""
Local DuckDB pipeline: dirty sources -> staging -> Kimball mart -> incremental load.

Usage:
    python pipeline.py init      # full build: staging + dimensions + facts
    python pipeline.py delta     # incremental load of the two delta files
    python pipeline.py test      # idempotency proof (runs delta twice, compares)
    python pipeline.py all       # init -> delta -> test

Layout expected:
    data/raw/raw_bookings.csv, raw_customers.csv, raw_pos_transactions.csv
    data/delta/delta_bookings_2024-04-01.csv, delta_customers_2024-04-01.csv
Output:
    mart.duckdb
"""
import re
import sys
from pathlib import Path

import duckdb
import pandas as pd

BASE = Path(__file__).resolve().parent
RAW_DIR = BASE / "data" / "raw"
DELTA_DIR = BASE / "data" / "delta"
DB_PATH = BASE / "mart.duckdb"

INITIAL_WATERMARK = "2024-03-28 00:00:00"


# --------------------------------------------------------------------------
# 1. Column-name normalisation.  camelCase / snake_case / spaces all collapse
#    to the same key, so one mapping handles every source variant.
# --------------------------------------------------------------------------
BOOKING_COLS = {
    "bookingid": "booking_id", "hotelname": "hotel_name", "roomtype": "room_type",
    "customerid": "customer_id", "customername": "customer_name",
    "customeremail": "customer_email", "checkindate": "check_in_date",
    "checkoutdate": "check_out_date", "nights": "nights",
    "bookingstatus": "booking_status", "channel": "channel",
    "bookingsource": "booking_source", "ratepernight": "rate_per_night",
    "currency": "currency", "totalrevenueidr": "total_revenue_idr",
    "discountpct": "discount_pct", "isloyaltymember": "is_loyalty_member",
    "createdat": "created_at", "updatedat": "updated_at",
}
CUSTOMER_COLS = {
    "customerid": "customer_id", "fullname": "full_name", "email": "email",
    "phone": "phone", "city": "city", "nationality": "nationality",
    "loyaltytier": "loyalty_tier", "joineddate": "joined_date",
    "updatedat": "updated_at",
}
POS_COLS = {
    "postransactionid": "pos_transaction_id", "outletname": "outlet_name",
    "hotelname": "hotel_name", "transactiondate": "transaction_date",
    "customerid": "customer_id", "itemname": "item_name", "category": "category",
    "quantity": "quantity", "unitprice": "unit_price", "totalamount": "total_amount",
    "paymentmethod": "payment_method", "staffid": "staff_id",
}

MARKETING_COLS = {
    "rowid": "row_id", "campaignname": "campaign_name", "platform": "platform",
    "hotel": "hotel_name", "date": "date", "impressions": "impressions",
    "clicks": "clicks", "spendidr": "spend_idr", "conversions": "conversions",
    "bookingsgenerated": "bookings_generated",
    "revenuegeneratedidr": "revenue_generated_idr",
    "adcreativeid": "ad_creative_id", "targetaudience": "target_audience",
}


def load_csv(path: Path, colmap: dict) -> pd.DataFrame:
    """Read a CSV as all-strings, normalise headers, rename to canonical names."""
    df = pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""])
    df.columns = [re.sub(r"[^a-z0-9]", "", str(c).lower()) for c in df.columns]
    df = df.rename(columns=colmap)
    expected = list(dict.fromkeys(colmap.values()))
    for c in expected:                     # tolerate a missing optional column
        if c not in df.columns:
            df[c] = None
    return df[expected]


def find_one(folder: Path, pattern: str) -> Path:
    hits = sorted(folder.glob(pattern))
    if not hits:
        raise FileNotFoundError(f"No file matching {pattern} in {folder}")
    return hits[0]


# --------------------------------------------------------------------------
# 2. Connection + reusable SQL macros
# --------------------------------------------------------------------------
MACROS = """
CREATE OR REPLACE MACRO parse_date(s) AS COALESCE(
    TRY_STRPTIME(s, '%Y-%m-%d')::DATE,
    TRY_STRPTIME(s, '%d/%m/%Y')::DATE,
    TRY_STRPTIME(s, '%d %b %Y')::DATE,
    TRY_STRPTIME(s, '%d-%b-%Y')::DATE,
    TRY_STRPTIME(s, '%Y/%m/%d')::DATE,
    TRY_STRPTIME(s, '%m/%d/%Y')::DATE
);
CREATE OR REPLACE MACRO parse_ts(s) AS COALESCE(
    TRY_STRPTIME(s, '%Y-%m-%d %H:%M:%S'),
    TRY_STRPTIME(s, '%Y-%m-%dT%H:%M:%S'),
    TRY_STRPTIME(s, '%Y-%m-%d %H:%M')::TIMESTAMP,
    TRY_STRPTIME(s, '%Y-%m-%d')::TIMESTAMP
);
CREATE OR REPLACE MACRO money(s) AS
    TRY_CAST(REPLACE(REPLACE(UPPER(TRIM(s)), 'IDR', ''), ',', '') AS DOUBLE);

CREATE OR REPLACE MACRO title_case(s) AS (
    array_to_string(
        list_transform(
            string_split(s, ' '),
            w -> CASE
                WHEN LEN(w) = 0 THEN w
                WHEN SUBSTR(w, 1, 1) = '(' THEN
                    '(' || UPPER(SUBSTR(w, 2, 1)) || LOWER(SUBSTR(w, 3))
                WHEN CONTAINS(w, '-') THEN
                    array_to_string(
                        list_transform(
                            string_split(w, '-'),
                            p -> UPPER(SUBSTR(p, 1, 1)) || LOWER(SUBSTR(p, 2))
                        ),
                        '-'
                    )
                ELSE
                    UPPER(SUBSTR(w, 1, 1)) || LOWER(SUBSTR(w, 2))
            END
        ),
        ' '
    )
);
"""


def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(str(DB_PATH))
    con.execute("SET timezone='Asia/Jakarta';")   # NEW
    con.execute(MACROS)
    return con


# --------------------------------------------------------------------------
# 3. Staging SQL (reused for raw *and* delta files -> one cleaning rule set)
# --------------------------------------------------------------------------
BOOKING_STG = r"""
CREATE OR REPLACE TABLE {out} AS
WITH typed AS (
    SELECT
        NULLIF(TRIM(booking_id), '')            AS booking_id,
        NULLIF(TRIM(hotel_name), '')            AS hotel_name,
        NULLIF(TRIM(room_type), '')             AS room_type,
        NULLIF(TRIM(customer_id), '')           AS customer_id,
        NULLIF(TRIM(customer_name), '')         AS customer_name,
        NULLIF(REGEXP_REPLACE(LOWER(TRIM(customer_email)), '\s+', '', 'g'), '') AS customer_email,
        parse_date(check_in_date)               AS check_in_date,
        parse_date(check_out_date)              AS check_out_date,
        TRY_CAST(nights AS INTEGER)             AS nights,                 -- source value, no computation
        NULLIF(TRIM(booking_status), '')        AS booking_status_raw,     -- preserved as-is
        NULLIF(TRIM(channel), '')               AS channel,
        NULLIF(TRIM(booking_source), '')        AS booking_source,
        TRY_CAST(rate_per_night AS DOUBLE)      AS rate_per_night,         -- source value
        NULLIF(TRIM(currency), '')              AS currency,               -- source value
        TRY_CAST(total_revenue_idr AS DOUBLE)   AS total_revenue_idr,      -- source value, null if null
        TRY_CAST(discount_pct AS DOUBLE)        AS discount_pct,           -- source value, no clamping
        NULLIF(TRIM(is_loyalty_member), '')     AS is_loyalty_member_raw,  -- preserved as-is
        parse_ts(created_at)                    AS source_created_at,
        COALESCE(parse_ts(updated_at), parse_ts(created_at)) AS source_updated_at
    FROM {src}
),
flagged AS (
    SELECT
        booking_id, hotel_name, room_type, customer_id, customer_name,
        LOWER(customer_email) AS customer_email,
        check_in_date, check_out_date, nights,
        CASE
            WHEN booking_status_raw IS NULL OR TRIM(booking_status_raw) = '' THEN NULL
            ELSE (
                CASE REGEXP_REPLACE(LOWER(TRIM(booking_status_raw)), '[_\-]+', ' ', 'g')
                    WHEN 'checked out' THEN 'Checked-Out'
                    WHEN 'checkedout'  THEN 'Checked-Out'
                    WHEN 'checked in'  THEN 'Checked-In'
                    WHEN 'checkedin'   THEN 'Checked-In'
                    WHEN 'cancel'      THEN 'Cancelled'
                    WHEN 'cancelled'   THEN 'Cancelled'
                    WHEN 'canceled'    THEN 'Cancelled'
                    WHEN 'cancelation' THEN 'Cancelled'
                    WHEN 'cancellation' THEN 'Cancelled'
                    WHEN 'cnx' THEN 'Cancelled'
                    WHEN 'no show'     THEN 'No-Show'
                    WHEN 'noshow'      THEN 'No-Show'
                    WHEN 'confirm'     THEN 'Confirmed'
                    WHEN 'confirmed'   THEN 'Confirmed'
                    WHEN 'completed'   THEN 'Completed'
                    WHEN 'complete'    THEN 'Completed'
                    WHEN 'pending'     THEN 'Pending'
                    ELSE title_case(REGEXP_REPLACE(LOWER(TRIM(booking_status_raw)), '[_\-]+', ' ', 'g'))
                END
            )
        END AS booking_status,
        booking_status_raw,
        CASE
            WHEN channel IS NULL OR TRIM(channel) = '' THEN NULL
            ELSE (
                CASE REGEXP_REPLACE(LOWER(TRIM(channel)), '[_\-]+', ' ', 'g')
                    WHEN 'travel agent'   THEN 'Travel Agent'
                    WHEN 'corporate'      THEN 'Corporate'
                    WHEN 'walk in'        THEN 'Walk-In'
                    WHEN 'ota'            THEN 'OTA'
                    WHEN 'direct website' THEN 'Direct Website'
                    WHEN 'direct'         THEN 'Direct Website'
                    ELSE title_case(REGEXP_REPLACE(LOWER(TRIM(channel)), '[_\-]+', ' ', 'g'))
                END
            )
        END AS channel,
        booking_source, rate_per_night, currency, total_revenue_idr,discount_pct,
        CASE LOWER(is_loyalty_member_raw)
            WHEN '1' THEN TRUE  WHEN 'true'  THEN TRUE  WHEN 'yes' THEN TRUE
            WHEN 'y' THEN TRUE  WHEN 't'     THEN TRUE
            WHEN '0' THEN FALSE WHEN 'false' THEN FALSE WHEN 'no'  THEN FALSE
            WHEN 'n' THEN FALSE WHEN 'f'     THEN FALSE
            ELSE NULL END                       AS is_loyalty_member,
        is_loyalty_member_raw,
        source_created_at, source_updated_at,
        (booking_id IS NULL)                                 AS dq_missing_pk,
        (hotel_name IS NULL)                                 AS dq_missing_hotel,
        (check_in_date IS NULL OR check_out_date IS NULL)    AS dq_bad_date,
        (check_in_date > check_out_date)                     AS dq_negative_stay,
        (total_revenue_idr IS NULL)                          AS dq_missing_revenue
    FROM typed
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY booking_id
        ORDER BY source_updated_at DESC NULLS LAST, source_created_at DESC NULLS LAST
    ) AS rn
    FROM flagged
    WHERE NOT dq_missing_pk
)
SELECT * EXCLUDE (rn, booking_status_raw, is_loyalty_member_raw)
FROM deduped WHERE rn = 1
"""

CUSTOMER_STG = r"""
CREATE OR REPLACE TABLE {out} AS
WITH typed AS (
    SELECT
        UPPER(NULLIF(TRIM(customer_id), ''))  AS customer_id,
        NULLIF(TRIM(full_name), '')           AS full_name,
        -- clean: lowercase + remove all internal whitespace
        NULLIF(REGEXP_REPLACE(LOWER(TRIM(email)), '\s+', '', 'g'), '') AS email_clean,
        NULLIF(TRIM(phone), '')               AS phone,
        NULLIF(TRIM(city), '')                AS city,
        NULLIF(TRIM(nationality), '')         AS nationality,
        NULLIF(TRIM(loyalty_tier), '')        AS loyalty_tier_raw,
        parse_date(joined_date)               AS joined_date,
        parse_ts(updated_at)                  AS source_updated_at
    FROM {src}
),
repaired AS (
    SELECT *,
        CASE
            WHEN email_clean IS NULL                THEN NULL
            WHEN CONTAINS(email_clean, '@')         THEN email_clean
            -- repair missing '@' before known domains
            ELSE COALESCE(
                REGEXP_REPLACE(
                    email_clean,
                    '^(.*?)(gmail|yahoo|hotmail|outlook|live|icloud|aol|protonmail)\.com$',
                    '\1@\2.com'
                ),
                email_clean                          -- if no match, keep as-is
            )
        END AS email
    FROM typed
),
flagged AS (
    SELECT
        customer_id, full_name, email, phone, city, nationality,
        CASE LOWER(loyalty_tier_raw)
            WHEN 'bronze'   THEN 'Bronze'
            WHEN 'silver'   THEN 'Silver'
            WHEN 'gold'     THEN 'Gold'
            WHEN 'platinum' THEN 'Platinum'
            ELSE NULL END              AS loyalty_tier,
        joined_date, source_updated_at,
        (customer_id IS NULL) AS dq_missing_pk,
        -- bad only if nothing after '@' (or no '@' at all)
        (email IS NULL OR email NOT LIKE '%_@_%') AS dq_bad_email
    FROM repaired
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY customer_id
        ORDER BY source_updated_at DESC NULLS LAST, joined_date DESC NULLS LAST
    ) AS rn
    FROM flagged
    WHERE NOT dq_missing_pk
)
SELECT * EXCLUDE (rn) FROM deduped WHERE rn = 1
"""

POS_STG = r"""
CREATE OR REPLACE TABLE {out} AS
WITH typed AS (
    SELECT
        UPPER(NULLIF(TRIM(pos_transaction_id), '')) AS pos_transaction_id,
        NULLIF(TRIM(outlet_name), '')        AS outlet_name,
        COALESCE(NULLIF(TRIM(hotel_name), ''),
                 NULLIF(TRIM(SPLIT_PART(COALESCE(outlet_name, ''), ' - ', 1)), '')) AS hotel_name,
        parse_date(transaction_date)         AS transaction_date,
        NULLIF(TRIM(customer_id), '')        AS customer_id,
        CASE
            WHEN item_name IS NULL OR TRIM(item_name) = '' THEN NULL
            ELSE title_case(
                CASE LOWER(REPLACE(TRIM(item_name), '_', ' '))
                    WHEN 'kopi'       THEN 'coffee'
                    WHEN 'coffie'     THEN 'coffee'
                    WHEN 'cock tail'  THEN 'cocktail'
                    WHEN 'coctail'    THEN 'cocktail'
                    WHEN 'nasigoreng' THEN 'nasi goreng'
                    ELSE LOWER(REPLACE(TRIM(item_name), '_', ' '))
                END
            )
        END AS item_name,
        CASE
            WHEN category IS NULL OR TRIM(category) = '' THEN 'Uncategorised'
            ELSE title_case(LOWER(TRIM(category)))
        END AS category,
        TRY_CAST(quantity AS INTEGER)        AS quantity,
        money(unit_price)                    AS unit_price,
        money(total_amount)                  AS total_amount,
        CASE
            WHEN payment_method IS NULL OR TRIM(payment_method) = '' THEN NULL
            ELSE (
                CASE REGEXP_REPLACE(LOWER(TRIM(payment_method)), '[_\-]+', ' ', 'g')
                    WHEN 'cc'           THEN 'Credit Card'
                    WHEN 'credit card'  THEN 'Credit Card'
                    WHEN 'creditcard'   THEN 'Credit Card'
                    WHEN 'credit'       THEN 'Credit Card'
                    WHEN 'gopay'        THEN 'GoPay'
                    WHEN 'ovo'          THEN 'OVO'
                    WHEN 'qris'         THEN 'QRIS'
                    WHEN 'dana'         THEN 'Dana'
                    WHEN 'debit card'   THEN 'Debit Card'
                    WHEN 'bank transfer' THEN 'Bank Transfer'
                    ELSE title_case(REGEXP_REPLACE(LOWER(TRIM(payment_method)), '[_\-]+', ' ', 'g'))
                END
            )
        END AS payment_method,
        NULLIF(TRIM(staff_id), '')           AS staff_id
    FROM {src}
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY pos_transaction_id ORDER BY 1) AS rn
    FROM typed WHERE pos_transaction_id IS NOT NULL
)
SELECT
    pos_transaction_id, outlet_name, hotel_name, transaction_date, customer_id,
    item_name, category, quantity, unit_price,
    COALESCE(total_amount, quantity * unit_price) AS total_amount,
    payment_method, staff_id,
    (pos_transaction_id IS NULL OR transaction_date IS NULL OR customer_id IS NULL) AS dq_flag
FROM deduped WHERE rn = 1

"""

MARKETING_STG = r"""
CREATE OR REPLACE TABLE {out} AS
WITH typed AS (
    SELECT
        TRY_CAST(row_id AS INTEGER)         AS row_id,
        NULLIF(TRIM(campaign_name), '')     AS campaign_name,
        NULLIF(TRIM(platform), '')          AS platform_raw,
        NULLIF(TRIM(hotel_name), '')        AS hotel_name,
        parse_date(date)                    AS date,
        TRY_CAST(impressions AS BIGINT)     AS impressions,
        TRY_CAST(clicks AS BIGINT)          AS clicks,
        money(spend_idr)                    AS spend_idr,
        TRY_CAST(conversions AS BIGINT)     AS conversions,
        TRY_CAST(bookings_generated AS BIGINT) AS bookings_generated,
        money(revenue_generated_idr)        AS revenue_generated_idr,
        NULLIF(TRIM(ad_creative_id), '')    AS ad_creative_id,
        NULLIF(TRIM(target_audience), '')   AS target_audience
    FROM {src}
),
flagged AS (
    SELECT
        row_id, campaign_name,
        CASE LOWER(platform_raw)
            WHEN 'email campaign'   THEN 'Email Campaign'
            WHEN 'tiktok ads'       THEN 'TikTok Ads'
            WHEN 'google ads'       THEN 'Google Ads'
            WHEN 'meta ads'         THEN 'Meta Ads'
            WHEN 'facebook ads'     THEN 'Meta Ads'
            WHEN 'instagram ads'    THEN 'Meta Ads'
            ELSE title_case(platform_raw) END AS platform,
        hotel_name, date, impressions, clicks, spend_idr, conversions,
        bookings_generated, revenue_generated_idr, ad_creative_id, target_audience,
        (row_id IS NULL)        AS dq_missing_pk,
        (date IS NULL)          AS dq_bad_date,
        (campaign_name IS NULL) AS dq_missing_campaign
    FROM typed
),
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY campaign_name, platform, ad_creative_id,
                        target_audience, date, hotel_name
            ORDER BY row_id DESC NULLS LAST
        ) AS rn
    FROM flagged
)
SELECT * EXCLUDE (rn) FROM deduped WHERE rn = 1
"""


def build_staging(con, raw: bool, suffix: str = ""):
    """Build stg_* (raw=True) or stg_delta_* (raw=False) tables."""
    out = "stg" if raw else "stg_delta"
    folder = RAW_DIR if raw else DELTA_DIR

    con.register("in_bookings", load_csv(
        find_one(folder, "raw_bookings*.csv" if raw else "delta_bookings*.csv"), BOOKING_COLS))
    con.register("in_customers", load_csv(
        find_one(folder, "raw_customers*.csv" if raw else "delta_customers*.csv"), CUSTOMER_COLS))

    for sql, src, tbl in (
        (BOOKING_STG, "in_bookings", f"{out}_bookings"),
        (CUSTOMER_STG, "in_customers", f"{out}_customers"),
    ):
        con.execute(sql.replace("{src}", src).replace("{out}", tbl))

    if raw:  # POS has no delta file in this task
        con.register("in_pos", load_csv(find_one(folder, "raw_pos_transactions*.csv"), POS_COLS))
        con.execute(POS_STG.replace("{src}", "in_pos").replace("{out}", "stg_pos"))

    if raw:  # marketing has no delta file
        try:
            con.register("in_marketing", load_csv(
                find_one(folder, "raw_marketing_performance*.csv"), MARKETING_COLS))
            con.execute(MARKETING_STG.replace("{src}", "in_marketing").replace("{out}", "stg_marketing"))
        except FileNotFoundError:
            pass  # bonus file optional


# --------------------------------------------------------------------------
# 4. Kimball mart DDL
# --------------------------------------------------------------------------
DDL = """
CREATE TABLE IF NOT EXISTS dim_hotel (
    hotel_sk UBIGINT PRIMARY KEY, hotel_name VARCHAR NOT NULL,
    created_at TIMESTAMP, updated_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS dim_date (
    date_sk INTEGER PRIMARY KEY, date DATE, year INTEGER, quarter INTEGER,
    month INTEGER, month_name VARCHAR, day INTEGER, day_name VARCHAR,
    day_of_week INTEGER, is_weekend BOOLEAN,
    created_at TIMESTAMP, updated_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_sk UBIGINT PRIMARY KEY,
    customer_id VARCHAR NOT NULL,
    full_name VARCHAR, email VARCHAR, phone VARCHAR, city VARCHAR, nationality VARCHAR,
    loyalty_tier VARCHAR, joined_date DATE,
    valid_from TIMESTAMP, valid_to TIMESTAMP, is_current BOOLEAN,
    source_updated_at TIMESTAMP,
    created_at TIMESTAMP, updated_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS fact_bookings (
    booking_sk UBIGINT PRIMARY KEY, booking_id VARCHAR NOT NULL,
    customer_sk UBIGINT, hotel_sk UBIGINT,
    check_in_date_sk INTEGER, check_out_date_sk INTEGER,
    nights INTEGER, booking_status VARCHAR, channel VARCHAR, booking_source VARCHAR,
    total_revenue_idr DOUBLE, discount_pct DOUBLE,
    source_created_at TIMESTAMP, source_updated_at TIMESTAMP,
    created_at TIMESTAMP, updated_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS fact_pos_transactions (
    pos_sk UBIGINT PRIMARY KEY, pos_transaction_id VARCHAR NOT NULL,
    customer_sk UBIGINT, hotel_sk UBIGINT, product_sk UBIGINT, date_sk INTEGER,
    outlet_name VARCHAR,
    quantity INTEGER, unit_price_idr DOUBLE, total_amount_idr DOUBLE,
    payment_method VARCHAR, staff_id VARCHAR,
    created_at TIMESTAMPTZ, updated_at TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS etl_watermark (
    source_name VARCHAR PRIMARY KEY, last_watermark TIMESTAMP, updated_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS dim_product (
    product_sk UBIGINT PRIMARY KEY,
    item_name  VARCHAR NOT NULL,
    category   VARCHAR,
    created_at TIMESTAMPTZ, updated_at TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS dim_campaign (
    campaign_sk UBIGINT PRIMARY KEY,
    campaign_name VARCHAR NOT NULL,
    platform      VARCHAR,
    hotel_sk      UBIGINT,
    ad_creative_id VARCHAR,
    target_audience VARCHAR,
    created_at TIMESTAMPTZ, updated_at TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS fact_marketing_daily (
    marketing_sk UBIGINT PRIMARY KEY,
    campaign_sk  UBIGINT,
    hotel_sk     UBIGINT,
    date_sk      INTEGER,
    impressions  BIGINT,
    clicks       BIGINT,
    spend_idr    DOUBLE,
    conversions  BIGINT,
    bookings_generated BIGINT,
    revenue_generated_idr DOUBLE,
    created_at TIMESTAMPTZ, updated_at TIMESTAMPTZ
);
"""

# Customer version valid at a point in time; falls back to the current version.
CUSTOMER_ASOF = """
COALESCE(
    (SELECT c.customer_sk FROM dim_customer c
      WHERE c.customer_id = {cust_id}
        AND {as_of} >= c.valid_from
        AND {as_of} < COALESCE(c.valid_to, TIMESTAMP '9999-12-31')
      ORDER BY c.valid_from DESC LIMIT 1),
    (SELECT c.customer_sk FROM dim_customer c
      WHERE c.customer_id = {cust_id} AND c.is_current LIMIT 1),
    CAST(0 AS UBIGINT)
)
"""


def build_mart(con):
    con.execute(DDL)

    # ---- dim_date : Q1 + Q2 2024 -----------------------------------------
    con.execute("""
        INSERT INTO dim_date
        SELECT CAST(STRFTIME(d, '%Y%m%d') AS INTEGER), d, YEAR(d), QUARTER(d),
               MONTH(d), STRFTIME(d, '%B'), DAY(d), STRFTIME(d, '%A'),
               DAYOFWEEK(d), DAYOFWEEK(d) IN (0, 6),
               CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        FROM (SELECT UNNEST(generate_series(DATE '2024-01-01', DATE '2024-06-30',
                                            INTERVAL 1 DAY)) AS d)
        WHERE NOT EXISTS (SELECT 1 FROM dim_date)
    """)

    # ---- dim_hotel : hash surrogate key = stable across runs -------------
    con.execute("""
        INSERT INTO dim_hotel
        SELECT hash(name), name, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        FROM (
            SELECT MIN(hotel_name) AS name
            FROM (
                SELECT hotel_name FROM stg_bookings WHERE hotel_name IS NOT NULL
                UNION
                SELECT hotel_name FROM stg_pos      WHERE hotel_name IS NOT NULL
            )
            GROUP BY LOWER(TRIM(hotel_name))
        ) t
        WHERE NOT EXISTS (SELECT 1 FROM dim_hotel h WHERE LOWER(TRIM(h.hotel_name)) = LOWER(TRIM(t.name)))
    """)

    # ---- dim_customer : initial SCD2 version -----------------------------
    # No history available at initial load -> valid_from = joined_date,
    # so bookings made any time after signup resolve to this version.
    con.execute("""
        INSERT INTO dim_customer
        SELECT hash(customer_id || '|' || COALESCE(CAST(joined_date AS VARCHAR), 'na')),
        customer_id, full_name, email, phone, city, nationality,
               loyalty_tier, joined_date,
               COALESCE(joined_date::TIMESTAMP, TIMESTAMP '1900-01-01'),
               NULL, TRUE, source_updated_at, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        FROM stg_customers
        WHERE NOT EXISTS (SELECT 1 FROM dim_customer)
    """)

    # ---- dim_product (from POS) -----------------------------------------
    con.execute("""
        INSERT INTO dim_product
        SELECT hash(item_name || '|' || COALESCE(category, '')),
                item_name, category, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        FROM (
            SELECT item_name, category,
                    ROW_NUMBER() OVER (PARTITION BY item_name
                                        ORDER BY category NULLS LAST) AS rn
            FROM stg_pos
            WHERE item_name IS NOT NULL
        )
        WHERE rn = 1
    """)

    # ---- dim_campaign (from marketing) ----------------------------------
    con.execute("""
        INSERT INTO dim_campaign
        SELECT
            hash(m.campaign_name || '|' || COALESCE(m.platform, '') || '|' ||
                COALESCE(m.ad_creative_id, '') || '|' ||
                COALESCE(m.target_audience, '')),
            m.campaign_name, m.platform, NULL AS hotel_sk,
            m.ad_creative_id, m.target_audience,
            CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        FROM (
            SELECT DISTINCT campaign_name, platform, ad_creative_id, target_audience
            FROM stg_marketing WHERE campaign_name IS NOT NULL
        ) m
    """)

    # ---- fact_bookings ----------------------------------------------------
    con.execute(f"""
        INSERT INTO fact_bookings
        SELECT * EXCLUDE (rn) FROM (
            SELECT
                hash(b.booking_id) AS booking_sk,
                b.booking_id,
                {CUSTOMER_ASOF.format(cust_id='b.customer_id',
                                      as_of='COALESCE(b.source_created_at, b.check_in_date::TIMESTAMP)')} AS customer_sk,
                h.hotel_sk,
                CAST(STRFTIME(b.check_in_date,  '%Y%m%d') AS INTEGER) AS check_in_date_sk,
                CAST(STRFTIME(b.check_out_date, '%Y%m%d') AS INTEGER) AS check_out_date_sk,
                b.nights, b.booking_status, b.channel, b.booking_source,
                b.total_revenue_idr, b.discount_pct,
                b.source_created_at, b.source_updated_at,
                CURRENT_TIMESTAMP AS created_at, CURRENT_TIMESTAMP AS updated_at,
                ROW_NUMBER() OVER (
                    PARTITION BY b.booking_id
                    ORDER BY b.source_updated_at DESC NULLS LAST
                ) AS rn
            FROM stg_bookings b
            LEFT JOIN dim_hotel h ON h.hotel_name = b.hotel_name
        )
        WHERE rn = 1
    """)

    # ---- fact_pos_transactions -------------------------------------------
    con.execute(f"""
        INSERT INTO fact_pos_transactions
        SELECT
            hash(p.pos_transaction_id), p.pos_transaction_id,
            {CUSTOMER_ASOF.format(cust_id='p.customer_id',
                                  as_of='p.transaction_date::TIMESTAMP')},
            h.hotel_sk,
            pr.product_sk,
            CAST(STRFTIME(p.transaction_date, '%Y%m%d') AS INTEGER),
            p.outlet_name, p.quantity, p.unit_price,
            p.total_amount, p.payment_method, p.staff_id,
            CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        FROM stg_pos p
        LEFT JOIN dim_hotel   h  ON h.hotel_name  = p.hotel_name
        LEFT JOIN dim_product pr ON pr.item_name  = p.item_name
                                 AND pr.category   IS NOT DISTINCT FROM p.category
    """)

    # ---- fact_marketing_daily -------------------------------------------
    con.execute("""
        INSERT INTO fact_marketing_daily
        SELECT
            hash(
                CAST(m.row_id AS VARCHAR) || '|' ||
                COALESCE(m.campaign_name, '') || '|' ||
                COALESCE(m.platform, '') || '|' ||
                COALESCE(m.hotel_name, '') || '|' ||
                COALESCE(CAST(m.date AS VARCHAR), '') || '|' ||
                COALESCE(m.ad_creative_id, '') || '|' ||
                COALESCE(m.target_audience, '')
            ),
            c.campaign_sk,
            h.hotel_sk,                                   -- ← from dim_hotel
            CAST(STRFTIME(m.date, '%Y%m%d') AS INTEGER),
            m.impressions, m.clicks, m.spend_idr,
            m.conversions, m.bookings_generated, m.revenue_generated_idr,
            CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        FROM stg_marketing m
        LEFT JOIN dim_campaign c
                ON c.campaign_name    = m.campaign_name
                AND c.platform         = m.platform
                AND c.ad_creative_id   IS NOT DISTINCT FROM m.ad_creative_id
                AND c.target_audience  IS NOT DISTINCT FROM m.target_audience
        LEFT JOIN dim_hotel h
                ON h.hotel_name       = m.hotel_name        -- ← new join
    """)

    # ---- starting watermarks ---------------------------------------------
    con.execute(f"""
        INSERT INTO etl_watermark
        SELECT * FROM (VALUES
            ('bookings',  TIMESTAMP '{INITIAL_WATERMARK}', CURRENT_TIMESTAMP),
            ('customers', TIMESTAMP '{INITIAL_WATERMARK}', CURRENT_TIMESTAMP)
        ) v(source_name, last_watermark, updated_at)
        WHERE NOT EXISTS (SELECT 1 FROM etl_watermark)
    """)

    # ---- reporting views revenue per guest -------------------------------------------------
    con.execute("""
        CREATE OR REPLACE VIEW rpt_fnb_revenue_per_guest AS
        SELECT
            COALESCE(c.customer_id, 'ANONYMOUS') AS customer_id,
            COALESCE(c.full_name,   'Walk-in')   AS guest_name,
            COUNT(*)                             AS line_items,
            SUM(f.total_amount_idr)              AS fnb_revenue_idr
        FROM fact_pos_transactions f
        LEFT JOIN dim_customer c
               ON c.customer_sk = f.customer_sk AND c.is_current
        GROUP BY 1, 2
        ORDER BY fnb_revenue_idr DESC NULLS LAST
    """)

    # ---- reporting views monthly revenue by hotel -------------------------------------------------
    con.execute("""
        CREATE OR REPLACE VIEW rpt_monthly_revenue_by_hotel AS
        SELECT
            d.year,
            d.month,
            d.month_name,
            h.hotel_name,
            COUNT(*)                       AS bookings,
            SUM(f.total_revenue_idr)       AS revenue_idr
        FROM fact_bookings f
        JOIN dim_date  d ON d.date_sk  = f.check_in_date_sk
        JOIN dim_hotel h ON h.hotel_sk = f.hotel_sk
        GROUP BY 1, 2, 3, 4
        ORDER BY 1, 2, 6 DESC NULLS LAST
    """)


# --------------------------------------------------------------------------
# 5. Incremental load
# --------------------------------------------------------------------------
def filter_delta_by_watermark(con):
    """Keep only rows at/after the stored watermark.  '>=' (not '>') so a row
    that arrives with the same timestamp as the last run is not lost; the MERGE
    and the SCD2 no-op checks make the re-processing harmless."""
    for src, tbl in (("bookings", "stg_delta_bookings"), ("customers", "stg_delta_customers")):
        con.execute(f"""
            CREATE OR REPLACE TABLE {tbl}_wm AS
            SELECT d.* FROM {tbl} d
            WHERE d.source_updated_at >= (
                SELECT last_watermark FROM etl_watermark WHERE source_name = '{src}')
        """)
        con.execute(f"DROP TABLE {tbl}")
        con.execute(f"ALTER TABLE {tbl}_wm RENAME TO {tbl}")


def merge_bookings(con) -> int:
    before = con.execute("SELECT count(*) FROM fact_bookings").fetchone()[0]
    con.execute(f"""
        MERGE INTO fact_bookings AS t
        USING (
            SELECT
                hash(b.booking_id) AS booking_sk,
                b.booking_id,
                {CUSTOMER_ASOF.format(cust_id='b.customer_id',
                                      as_of='COALESCE(b.source_created_at, b.check_in_date::TIMESTAMP)')} AS customer_sk,
                h.hotel_sk,
                CAST(STRFTIME(b.check_in_date,  '%Y%m%d') AS INTEGER) AS check_in_date_sk,
                CAST(STRFTIME(b.check_out_date, '%Y%m%d') AS INTEGER) AS check_out_date_sk,
                b.nights, b.booking_status, b.channel, b.booking_source, b.total_revenue_idr, b.discount_pct, b.source_created_at, b.source_updated_at
            FROM stg_delta_bookings b
            LEFT JOIN dim_hotel h ON h.hotel_name = b.hotel_name
        ) AS s
        ON t.booking_id = s.booking_id
        WHEN MATCHED AND s.source_updated_at > t.source_updated_at THEN UPDATE SET
            customer_sk        = s.customer_sk,
            hotel_sk           = s.hotel_sk,
            check_in_date_sk   = s.check_in_date_sk,
            check_out_date_sk  = s.check_out_date_sk,
            nights             = s.nights,
            booking_status     = s.booking_status,
            channel            = s.channel,
            booking_source     = s.booking_source,
            total_revenue_idr  = s.total_revenue_idr,
            discount_pct       = s.discount_pct,
            source_updated_at  = s.source_updated_at,
            updated_at         = CURRENT_TIMESTAMP
        WHEN NOT MATCHED THEN INSERT (
            booking_sk, booking_id, customer_sk, hotel_sk,
            check_in_date_sk, check_out_date_sk, nights, booking_status, channel,
            booking_source, total_revenue_idr, discount_pct, source_created_at, source_updated_at, created_at, updated_at
        ) VALUES (
            s.booking_sk, s.booking_id, s.customer_sk, s.hotel_sk,
            s.check_in_date_sk, s.check_out_date_sk, s.nights, s.booking_status, s.channel,
            s.booking_source, s.total_revenue_idr, s.discount_pct, s.source_created_at, s.source_updated_at,
            CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        )
    """)
    return con.execute("SELECT count(*) FROM fact_bookings").fetchone()[0] - before


def merge_customers(con) -> int:
    """SCD2 on loyalty_tier (steps 1+2), Type-1 in place for everything else (step 3).

    Each step is a no-op on a second run, which is what makes the whole thing
    idempotent.
    """
    # 0. ignore rows that are older than the current version (late/stale arrivals)
    con.execute("""
        CREATE OR REPLACE TABLE stg_delta_customers_fresh AS
        SELECT d.* FROM stg_delta_customers d
        WHERE d.source_updated_at > COALESCE(
            (SELECT c.source_updated_at FROM dim_customer c
              WHERE c.customer_id = d.customer_id AND c.is_current), TIMESTAMP '1900-01-01')
    """)
    con.execute("DROP TABLE stg_delta_customers")
    con.execute("ALTER TABLE stg_delta_customers_fresh RENAME TO stg_delta_customers")

    before = con.execute("SELECT count(*) FROM dim_customer").fetchone()[0]

    # 1. close the current version where the tier changed
    con.execute("""
        UPDATE dim_customer AS c
        SET valid_to = d.source_updated_at, is_current = FALSE, updated_at = CURRENT_TIMESTAMP
        FROM stg_delta_customers AS d
        WHERE c.customer_id = d.customer_id
          AND c.is_current
          AND c.loyalty_tier IS DISTINCT FROM d.loyalty_tier
    """)

    # 2. open a new version for every delta customer that now has no current row
    #    (this covers both a tier change and a brand-new customer)
    con.execute("""
        INSERT INTO dim_customer
        SELECT
            hash(d.customer_id || '|' || CAST(d.source_updated_at AS VARCHAR)),
            d.customer_id, d.full_name, d.email, d.phone, d.city, d.nationality,
            d.loyalty_tier, d.joined_date,
            d.source_updated_at, NULL, TRUE, d.source_updated_at,
            CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        FROM stg_delta_customers d
        WHERE NOT EXISTS (
            SELECT 1 FROM dim_customer c WHERE c.customer_id = d.customer_id AND c.is_current)
    """)

    # 3. Type-1 attributes updated in place (only when something actually differs)
    con.execute("""
        UPDATE dim_customer AS c
        SET full_name = d.full_name, email = d.email, phone = d.phone,
            city = d.city, nationality = d.nationality, joined_date = d.joined_date,
            source_updated_at = d.source_updated_at, updated_at = CURRENT_TIMESTAMP
        FROM stg_delta_customers AS d
        WHERE c.customer_id = d.customer_id
          AND c.is_current
          AND c.loyalty_tier IS NOT DISTINCT FROM d.loyalty_tier
          AND (c.full_name    IS DISTINCT FROM d.full_name
            OR c.email        IS DISTINCT FROM d.email
            OR c.phone        IS DISTINCT FROM d.phone
            OR c.city         IS DISTINCT FROM d.city
            OR c.nationality  IS DISTINCT FROM d.nationality
            OR c.joined_date  IS DISTINCT FROM d.joined_date)
    """)

    return con.execute("SELECT count(*) FROM dim_customer").fetchone()[0] - before


def advance_watermark(con):
    for src, tbl in (("bookings", "stg_delta_bookings"), ("customers", "stg_delta_customers")):
        con.execute(f"""
            UPDATE etl_watermark
            SET last_watermark = GREATEST(
                    last_watermark,
                    COALESCE((SELECT MAX(source_updated_at) FROM {tbl}), last_watermark)),
                updated_at = CURRENT_TIMESTAMP
            WHERE source_name = '{src}'
        """)


def refresh_new_hotels(con):
    """A delta booking can reference a hotel we have never seen."""
    con.execute("""
        INSERT INTO dim_hotel
        SELECT DISTINCT hash(b.hotel_name), b.hotel_name,
               CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        FROM stg_delta_bookings b
        WHERE b.hotel_name IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM dim_hotel h WHERE h.hotel_name = b.hotel_name)
    """)


def snapshot(con) -> dict:
    q = lambda s: con.execute(s).fetchone()[0]
    return {
        "fact_bookings": q("SELECT count(*) FROM fact_bookings"),
        "fact_pos_transactions": q("SELECT count(*) FROM fact_pos_transactions"),
        "fact_marketing_daily": q("SELECT count(*) FROM fact_marketing_daily"),
        "dim_customer": q("SELECT count(*) FROM dim_customer"),
        "dim_customer (Iscurrent = True)": q("SELECT count(*) FROM dim_customer WHERE is_current"),
        # "revenue_total": round(q("SELECT COALESCE(SUM(total_revenue_idr),0) FROM fact_bookings"), 2),
        # "revenue_march": round(q("""SELECT COALESCE(SUM(f.total_revenue_idr),0)
        #                            FROM fact_bookings f JOIN dim_date d ON d.date_sk = f.check_in_date_sk
        #                            WHERE d.year = 2024 AND d.month = 3"""), 2),
    }


# --------------------------------------------------------------------------
# 6. Commands
# --------------------------------------------------------------------------
def cmd_init(con):
    print("== INITIAL LOAD ==")
    build_staging(con, raw=True)
    build_mart(con)
    print(snapshot(con))

def cmd_delta(con):
    print("== INCREMENTAL LOAD ==")
    build_staging(con, raw=False)
    filter_delta_by_watermark(con)
    refresh_new_hotels(con)
    new_bk = merge_bookings(con)
    new_cu = merge_customers(con)
    advance_watermark(con)
    print(f"fact_bookings new rows: {new_bk} | dim_customer new versions: {new_cu}")
    print(snapshot(con))


def cmd_test(con):
    print("== IDEMPOTENCY TEST ==")
    before = snapshot(con)
    build_staging(con, raw=False)
    filter_delta_by_watermark(con)
    refresh_new_hotels(con)
    merge_bookings(con)
    merge_customers(con)
    advance_watermark(con)
    after = snapshot(con)
    print("before:", before)
    print("after :", after)
    ok = before == after
    print("IDEMPOTENT ✔" if ok else "NOT IDEMPOTENT �’")
    return ok


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "all"
    con = connect()
    try:
        if cmd == "init":
            cmd_init(con)
        elif cmd == "delta":
            cmd_delta(con)
        elif cmd == "test":
            cmd_test(con)
        else:
            cmd_init(con)
            cmd_delta(con)
            cmd_test(con)
    finally:
        con.close()


if __name__ == "__main__":
    main()