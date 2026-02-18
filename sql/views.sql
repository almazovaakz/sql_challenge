-- Make CDHDR usable: one timestamp column
CREATE VIEW IF NOT EXISTS v_hdr AS
SELECT
  MANDANT,OBJECTCLAS,OBJECTID,CHANGENR,datetime(substr(UDATE,1,10) || ' ' || substr(UTIME,12,8)) AS ts
FROM CDHDR;

-- EKPO, but with a generated SAP-style key for joining to CDPOS.TABKEY
CREATE VIEW IF NOT EXISTS v_items AS
SELECT
  *,
  printf('%02d%010d%05d',CAST(MANDT AS INTEGER),CAST(EBELN AS INTEGER),CAST(EBELP AS INTEGER)
  ) AS tabkey
FROM EKPO;

-- When item was inserted later (KEY + I)
CREATE VIEW IF NOT EXISTS v_item_insert AS
SELECT
  c.MANDANT,
  c.OBJECTID,
  c.TABKEY AS tabkey,
  MIN(h.ts) AS created_ts
FROM CDPOS c
JOIN v_hdr h
  ON h.MANDANT    = c.MANDANT
 AND h.OBJECTCLAS = c.OBJECTCLAS
 And h.OBJECTID   = c.OBJECTID
 And h.CHANGENR   = c.CHANGENR
WHERE c.TABNAME = 'EKPO'
  AND c.FNAME   = 'KEY'
  AND c.CHNGIND = 'I'
GROUP BY c.MANDANT, c.OBJECTID, c.TABKEY;

-- NETWR changes with timestamps
CREATE VIEW IF NOT EXISTS v_netwr_changes AS
SELECT
  c.MANDANT,
  c.OBJECTID,
  c.TABKEY AS tabkey,
  h.ts,
  CAST(c.VALUE_OLD AS REAL) AS old_val,
  CAST(c.VALUE_NEW AS REAL) AS new_val
FROM CDPOS c
JOIN v_hdr h
  ON h.MANDANT    = c.MANDANT
 AND h.OBJECTCLAS = c.OBJECTCLAS
 And h.OBJECTID   = c.OBJECTID
 And h.CHANGENR   = c.CHANGENR
WHERE c.TABNAME = 'EKPO'
  AND c.FNAME   = 'NETWR';
