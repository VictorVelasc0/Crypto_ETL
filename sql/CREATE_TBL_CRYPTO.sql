DROP TABLE IF EXISTS "data-engineer-database".dani_gt_10_coderhouse.crypto;
CREATE TABLE IF NOT EXISTS "data-engineer-database".dani_gt_10_coderhouse.crypto
(
Moneda varchar(30) distkey,
Base varchar(30),
Precio numeric,
checked_at timestamp
)
sortkey(checked_at); 