INSERT
INTO "schema"."table" ("pk0","pk1","val0","val1","has_default")
SELECT data."pk0",data."pk1",data."val0",data."val1",data."has_default"FROM (
  SELECT ? AS "pk0",? AS "pk1",? AS "val0",? AS "val1",CASE WHEN ? = 1 THEN ? ELSE expr() END AS "has_default"
  UNION SELECT ?,?,?,?,CASE WHEN ? = 1 THEN ? ELSE expr() END
) AS data
WHERE ("val0"> now() - INTERVAL '3600' SECOND)
AND ("val1"> now() - INTERVAL '1' SECOND)
ON DUPLICATE KEY UPDATE
"val0"=VALUES("val0"),"val1"=VALUES("val1"),"has_default"=VALUES("has_default")