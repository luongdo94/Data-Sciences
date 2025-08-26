SELECT t.*, sku.ArtikelCode, sku.Pflegekennzeichnung
FROM (
    (t_Art_Text_DE t
    INNER JOIN t_Art_MegaBase m ON t.ArtNr = m.ArtNr)
    INNER JOIN t_Art_Mega_SKU sku ON m.ArtNr = sku.ArtNr
)
WHERE
    m.Marke IN ('Corporate', 'EXCD', 'XO')
    AND sku.Hauptfarbe IS NOT NULL
    AND sku.ArtikelCode IN ({aid_placeholders});