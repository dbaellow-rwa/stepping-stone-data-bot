from typing import Dict

# ──────────────────────────────────────────────────────────────────────────────
# Minimal BigQuery config placeholders (set at runtime or via env)
# ──────────────────────────────────────────────────────────────────────────────
BQ_PROJECT = "{BQ_PROJECT}"
BQ_DATASET = "{BQ_DATASET}"

def FT(table: str) -> str:
    """Fully-qualified table name helper."""
    return f"{BQ_PROJECT}.{BQ_DATASET}.{table}"

# ──────────────────────────────────────────────────────────────────────────────
# Grocery Store: Fake Tables + Summaries
# ──────────────────────────────────────────────────────────────────────────────
TABLE_SUMMARIES: Dict[str, str] = {
    "fct_store_sales": (
        "Transactional line-item sales (one row per item per transaction). "
        "Use for revenue, units, baskets, promos, categories, and store/channel splits."
    ),
    "fct_inventory_daily": (
        "Daily inventory snapshots by store and SKU. "
        "Use for on-hand, on-order, stockouts, shrink, and days-of-supply."
    ),
}

# ──────────────────────────────────────────────────────────────────────────────
# Prompt: fct_store_sales (transactions)
# ──────────────────────────────────────────────────────────────────────────────
def FCT_STORE_SALES_PROMPT() -> str:
    return f"""Prompt for fct_store_sales:
`{FT('fct_store_sales')}`
Transactional line-item sales (one row per item sold per transaction).

Important columns (fake but realistic):
- transaction_id: Unique ID for a customer transaction
- transaction_ts: POS timestamp (UTC)
- business_date: Local store date (DATE)
- store_id / store_name: Store identifiers
- channel: 'in_store', 'curbside', 'delivery', etc.
- register_id: POS register identifier (nullable for e-comm)
- cashier_id: Associate handling the sale (nullable for e-comm)
- customer_id: Loyalty/customer identifier (nullable)
- basket_gross_amount: Total basket gross (pre-discount; repeated on each line)
- basket_net_amount: Total basket net (post-discount; repeated on each line)
- line_number: Line sequence within the transaction
- sku_id / upc: Item identifiers
- sku_name: Item description (e.g., "Organic Gala Apples 3lb")
- brand_name: Brand (nullable for produce/bulk)
- dept_name: High-level department (e.g., 'Produce', 'Dairy', 'Bakery')
- category_name / subcategory_name: Category rollups (e.g., 'Fruit' / 'Apples')
- unit_size: Pack/size text (e.g., '16 oz', '3 lb')
- qty: Units sold on the line (can be decimal for weighed items)
- unit_price: Regular price per unit
- promo_flag: TRUE if any promotion applied to the line
- promo_code: Promo identifier (nullable)
- discount_amount: Discount applied to the line (absolute)
- net_amount: Line total after discount (qty * unit_price - discount_amount)
- tax_amount: Sales tax on the line
- tender_type: Primary tender (e.g., 'credit', 'debit', 'ebt', 'cash')
- zipcode: Store ZIP (for regional slicing)

Starter queries:

-- 1) Top 10 SKUs by revenue last 7 days
SELECT
  sku_id,
  sku_name,
  SUM(net_amount) AS revenue_7d,
  SUM(qty) AS units_7d
FROM {FT('fct_store_sales')}
WHERE business_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY sku_id, sku_name
ORDER BY revenue_7d DESC
LIMIT 10;

-- 2) Category performance (revenue, units, promo mix) by channel yesterday
SELECT
  channel,
  dept_name,
  category_name,
  SUM(net_amount) AS revenue,
  SUM(qty) AS units,
  SAFE_DIVIDE(SUM(IF(promo_flag, net_amount, 0)), SUM(net_amount)) AS promo_share
FROM {FT('fct_store_sales')}
WHERE business_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
GROUP BY channel, dept_name, category_name
ORDER BY revenue DESC;

-- 3) Basket metrics (AOV, items/basket) for last 30 days
WITH baskets AS (
  SELECT
    transaction_id,
    ANY_VALUE(business_date) AS business_date,
    ANY_VALUE(store_id) AS store_id,
    ANY_VALUE(channel) AS channel,
    MAX(basket_net_amount) AS basket_net,  -- repeated per line; take max
    SUM(qty) AS items
  FROM {FT('fct_store_sales')}
  WHERE business_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  GROUP BY transaction_id
)
SELECT
  channel,
  AVG(basket_net) AS avg_order_value,
  AVG(items) AS avg_items_per_basket,
  COUNT(*) AS basket_count
FROM baskets
GROUP BY channel
ORDER BY avg_order_value DESC;

Notes:
- Use business_date for daily rollups; use transaction_ts for intraday.
- For basket-level metrics, aggregate per transaction_id first.
- Do not infer category from sku_name; use dept/category fields.
"""

# ──────────────────────────────────────────────────────────────────────────────
# Prompt: fct_inventory_daily (inventory snapshots)
# ──────────────────────────────────────────────────────────────────────────────
def FCT_INVENTORY_DAILY_PROMPT() -> str:
    return f"""Prompt for fct_inventory_daily:
`{FT('fct_inventory_daily')}`
Daily end-of-day inventory snapshots by store and SKU.

Important columns (fake but realistic):
- snapshot_date: Inventory date (DATE, end-of-day)
- store_id / store_name: Store identifiers
- sku_id / upc: Item identifiers
- sku_name: Item description
- dept_name / category_name / subcategory_name: Merch rollups
- on_hand_units: Units physically on hand at end-of-day
- on_order_units: Units on open POs expected
- in_transit_units: Units shipped but not yet received
- safety_stock_units: Target buffer stock
- forecast_units_7d: 7-day demand forecast (rolling)
- shrink_units: Accumulated shrink/theft/damage units (period-to-date)
- last_receipt_date: Last date stock was received
- last_sale_date: Last date stock was sold (from sales fact)
- cost_per_unit: Weighted average cost
- retail_price: Current list price (nullable if price at POS only)
- supplier_id / supplier_name: Primary supplier (nullable)

Derived/risk metrics you can compute in SQL:
- days_of_supply = SAFE_DIVIDE(on_hand_units, NULLIF(forecast_units_7d/7, 0))
- stockout_flag = on_hand_units <= 0
- low_stock_flag = on_hand_units < safety_stock_units

Starter queries:

-- 1) Items at stockout or below safety stock today
SELECT
  store_id,
  sku_id,
  sku_name,
  on_hand_units,
  safety_stock_units,
  SAFE_DIVIDE(on_hand_units, NULLIF(forecast_units_7d/7, 0)) AS days_of_supply
FROM {FT('fct_inventory_daily')}
WHERE snapshot_date = CURRENT_DATE()
  AND (on_hand_units <= 0 OR on_hand_units < safety_stock_units)
ORDER BY on_hand_units ASC;

-- 2) Top 20 items by stockout risk (lowest days_of_supply) across all stores
SELECT
  sku_id,
  sku_name,
  SUM(on_hand_units) AS total_on_hand,
  SUM(forecast_units_7d) AS total_fcst_7d,
  SAFE_DIVIDE(SUM(on_hand_units), NULLIF(SUM(forecast_units_7d)/7, 0)) AS days_of_supply
FROM {FT('fct_inventory_daily')}
WHERE snapshot_date = CURRENT_DATE()
GROUP BY sku_id, sku_name
ORDER BY days_of_supply ASC NULLS FIRST
LIMIT 20;

-- 3) Aging inventory (no sales in >14 days but on_hand > 0)
SELECT
  store_id,
  sku_id,
  sku_name,
  on_hand_units,
  last_sale_date
FROM {FT('fct_inventory_daily')}
WHERE snapshot_date = CURRENT_DATE()
  AND on_hand_units > 0
  AND (last_sale_date IS NULL OR last_sale_date < DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY))
ORDER BY on_hand_units DESC;

Notes:
- Use snapshot_date for point-in-time inventory state.
- Join to sales only on (store_id, sku_id) and date ranges—avoid name-based joins.
- For chain-level views, aggregate across stores to get total days_of_supply.
"""

# ──────────────────────────────────────────────────────────────────────────────
# General SQL Guidelines (grocery flavor, still generic)
# ──────────────────────────────────────────────────────────────────────────────
def GENERAL_SQL_GUIDELINES() -> str:
    return """
- Prefer DATE columns (business_date, snapshot_date) for daily rollups.
- Aggregate to basket level for AOV/items-per-basket metrics before grouping.
- Use department/category fields for merchandising analysis; avoid parsing sku_name.
- For promo analysis, use promo_flag/discount_amount; avoid heuristics on sku_name.
- For stock health, compute days_of_supply with a rolling forecast; guard divides with NULLIF.
- When mixing sales and inventory, be explicit with grain:
  • Sales: (transaction_id, line_number)
  • Inventory: (store_id, sku_id, snapshot_date)
- For chain summaries, SUM across stores first, then compute ratios (e.g., days_of_supply).
"""

# ──────────────────────────────────────────────────────────────────────────────
# Export prompts in a mapping (mirrors your original pattern)
# ──────────────────────────────────────────────────────────────────────────────
def get_table_prompts() -> Dict[str, str]:
    return {
        "fct_store_sales": FCT_STORE_SALES_PROMPT(),
        "fct_inventory_daily": FCT_INVENTORY_DAILY_PROMPT(),
    }
