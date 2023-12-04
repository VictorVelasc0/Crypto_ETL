CREATE TABLE IF NOT EXISTS dani_gt_10_coderhouse.crypto_stg
(
Moneda varchar(256) primary key distkey,
Base varchar(256),
Precio float,
created_at timestamp
)
sortkey(created_at); 