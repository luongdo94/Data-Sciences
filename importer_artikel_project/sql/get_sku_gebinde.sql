SELECT 
    sku.ArtNr, 
    m.ArtikelNeu, 
    sku.ArtikelCode, 
    sku.Karton_Länge, 
    sku.Karton_Breite, 
    sku.Karton_Höhe, 
    m.Verpackungseinheit, 
    sku.Kartoneinheit,
    sku.Produktgewicht
FROM (t_Art_MegaBase m
            INNER JOIN t_Art_Mega_SKU sku ON m.ArtikelNeu = sku.ArtikelNeu)
            WHERE m.Marke IN ('Corporate', 'EXCD', 'XO')
            AND sku.ArtikelCode IN ({aid_placeholders})
            AND sku.Hauptfarbe is not null
            AND sku.FarbeNeu NOT LIKE '%/%'