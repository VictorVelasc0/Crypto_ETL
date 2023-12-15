CREATE TABLE IF NOT EXISTS dani_gt_10_coderhouse.crypto
(
Moneda varchar(256) distkey,
Base varchar(256),
Precio float,
created_at timestamp,
updated_at timestamp,
executed_at date
)
sortkey(created_at,updated_at,executed_at);
