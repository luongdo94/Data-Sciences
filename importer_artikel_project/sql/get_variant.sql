SELECT
    sku.ArtikelCode AS sku,
    m.ArtBasis AS aid,
    sku.Größe AS Größe,
    sku.Hauptfarbe AS Farbgruppe,
    sku.FarbeNeu AS Farbe,
    sku.isColorCombination AS zweifarbig,
    m.Grammatur AS Grammatur,
    m.Ursprungsland
FROM
    (
        (t_Art_Mega_SKU sku
        INNER JOIN t_Art_Text_DE t ON sku.ArtNr = t.ArtNr)
        INNER JOIN t_Art_MegaBase m ON sku.ArtNr = m.ArtNr
    )
    INNER JOIN t_Art_Flags f ON m.ArtNr = f.ArtNr
WHERE
    m.Marke IN ('Corporate', 'EXCD', 'XO')
    AND sku.Hauptfarbe IS NOT NULL
    AND m.ArtBasis IN ({aid_placeholders})