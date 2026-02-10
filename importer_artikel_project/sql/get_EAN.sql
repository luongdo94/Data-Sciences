SELECT
    t_Art_MegaBase.Verpackungseinheit,
    t_Art_Mega_SKU.ArtikelCode,
    tEANCodes.ArtNr,
    tEANCodes.EAN13,
    tEANCodes.QtyId,
    IIF(RIGHT(tEANCodes.ArtNr, 1) = 'S', 1, 0) AS IsEndsWithS
FROM
    (
        tEANCodes
        INNER JOIN t_Art_Mega_SKU ON (tEANCodes.SizeId = t_Art_Mega_SKU.SizeId)
        AND (tEANCodes.Farbe = t_Art_Mega_SKU.FarbCode)
    )
    INNER JOIN t_Art_MegaBase ON t_Art_Mega_SKU.ArtikelNeu = t_Art_MegaBase.ArtikelNeu
WHERE
    (
        (
            (tEANCodes.ArtNr = t_Art_Mega_SKU.ArtNr)
            OR (tEANCodes.ArtNr = t_Art_Mega_SKU.ArtNr + 'S')
        )
    )
    AND t_Art_Mega_SKU.FarbeNeu NOT LIKE '%/%'
    AND t_Art_MegaBase.Marke IN ('Corporate', 'EXCD', 'XO')
    AND t_Art_Mega_SKU.Hauptfarbe IS NOT NULL
    AND NOT (RIGHT(Trim(tEANCodes.ArtNr), 1) = 'S' AND tEANCodes.QtyId <> 1)

        