INSERT
INTO "schema"."table" ("pk0","pk1","val0","val1","has_default")
SELECT data."pk0",data."pk1",data."val0",data."val1",data."has_default"FROM (
  SELECT ? AS "pk0",? AS "pk1",? AS "val0",? AS "val1",CASE WHEN ? = 1 THEN ? ELSE expr() END AS "has_default"
  UNION SELECT ?,?,?,?,CASE WHEN ? = 1 THEN ? ELSE expr() END
) AS data
LEFT JOIN (SELECT "pk0","pk1", "table"."val1","table"."val0" FROM "schema"."table") AS current USING ("pk0","pk1")
WHERE (current."pk0" IS NULL OR
((data."val1",data."val0") > (current."val1",current."val0")))
ON DUPLICATE KEY UPDATE
"val0"=VALUES("val0"),"val1"=VALUES("val1"),"has_default"=VALUES("has_default")