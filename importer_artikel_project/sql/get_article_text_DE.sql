SELECT
    t.*,
    sku.Pflegekennzeichnung
FROM
    ((t_Art_Text_DE AS t
    INNER JOIN t_Art_MegaBase AS m ON t.ArtNr = m.ArtNr)
INNER JOIN t_Art_Mega_SKU AS sku ON m.ArtNr = sku.ArtNr)
WHERE
    m.Marke IN ('Corporate', 'EXCD', 'XO');