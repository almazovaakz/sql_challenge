WITH
params AS (
  -- INSERT VARIABLES HERE AS IN THE EXAMPLE BELOW
  SELECT AS mandt,  AS ebeln, '' AS d_date 
  -- SELECT 10 AS mandt, 71160672 AS ebeln, '2020-01-29' AS d_date
),
cutoff AS (
  SELECT datetime(d_date || ' 23:59:59') AS t, mandt, ebeln FROM params
),

-- PO header creation date = default existence date for items
po_created AS (
  SELECT datetime(substr(AEDAT,1,10) || ' 00:00:00') AS po_time
  FROM EKKO
  WHERE MANDT = (SELECT mandt FROM cutoff)
    AND EBELN = (SELECT ebeln FROM cutoff)
),

-- items for this PO (+ tabkey from the view)
items AS (
  SELECT *
  FROM v_items
  WHERE MANDT = (SELECT mandt FROM cutoff)
    AND EBELN = (SELECT ebeln FROM cutoff)
),

-- figure out when each item starts existing
items_with_time AS (
  SELECT
    i.*, COALESCE(ins.created_ts, (SELECT po_time FROM po_created)) AS item_time
  FROM items i
  LEFT JOIN v_item_insert ins
    ON ins.MANDANT = i.MANDT
   AND ins.OBJECTID = i.EBELN
   AND ins.tabkey = i.tabkey
),

-- last NETWR change at/before the cutoff (per item)
last_change AS (
  SELECT tabkey, new_val AS netwr_asof
  FROM (
    SELECT
      nc.*, ROW_NUMBER() OVER (PARTITION BY tabkey ORDER BY ts DESC) AS rn
    FROM v_netwr_changes nc
    WHERE nc.MANDANT = (SELECT mandt FROM cutoff)
      AND nc.OBJECTID = (SELECT ebeln FROM cutoff)
      AND nc.ts <= (SELECT t FROM cutoff)
  )
  WHERE rn = 1
),

-- initial NETWR: earliest old_val if there were changes, else EKPO.NETWR
initial_netwr AS (
  SELECT
    i.tabkey,
    COALESCE(
      (SELECT old_val FROM v_netwr_changes nc
       WHERE nc.MANDANT = i.MANDT
         AND nc.OBJECTID = i.EBELN
         AND nc.tabkey = i.tabkey
       ORDER BY nc.ts ASC
       LIMIT 1),
      CAST(i.NETWR AS REAL)
    ) AS netwr_initial
  FROM items i
),

-- choose final netwr per item, but only for items that exist before cutoff
final_items AS (
  SELECT
    it.tabkey,
    COALESCE(lc.netwr_asof, ini.netwr_initial) AS netwr_asof
  FROM items_with_time it
  LEFT JOIN last_change lc ON lc.tabkey = it.tabkey
  LEFT JOIN initial_netwr ini ON ini.tabkey = it.tabkey
  WHERE it.item_time <= (SELECT t FROM cutoff)
)

SELECT ROUND(SUM(netwr_asof), 2) AS TOTAL_NETWR
FROM final_items;
