CREATE TABLE IF NOT EXISTS "data-engineer-database".dani_gt_10_coderhouse.tbl_crypto
(
Moneda varchar(256) distkey,
Base varchar(256),
Precio float,
created_at timestamp,
updated_at timestamp,
executed_at timestamp
)
sortkey(created_at,updated_at,executed_at); 