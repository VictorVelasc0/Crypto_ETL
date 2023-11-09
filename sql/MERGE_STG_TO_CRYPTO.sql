MERGE INTO "data-engineer-database".dani_gt_10_coderhouse.tbl_crypto
USING "data-engineer-database".dani_gt_10_coderhouse.stg_crypto
ON tbl_crypto.Moneda=stg_crypto.Moneda AND tbl_crypto.created_at=stg_crypto.created_at
WHEN MATCHED THEN
    UPDATE SET 
    Precio = stg_crypto.Precio,
    created_at = stg_crypto.created_at,
    updated_at = GETDATE(),
    executed_at = GETDATE()
WHEN NOT MATCHED THEN
    INSERT (Moneda, Base, Precio, created_at, updated_at, executed_at)
    VALUES (stg_crypto.Moneda, stg_crypto.Base, stg_crypto.Precio, stg_crypto.created_at, GETDATE(), GETDATE())
