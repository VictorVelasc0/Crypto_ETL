CREATE TABLE IF NOT EXISTS "data-engineer-database".dani_gt_10_coderhouse.stg_crypto
(
Moneda varchar(256) primary key distkey,
Base varchar(256),
Precio float,
created_at timestamp
)
sortkey(created_at); 