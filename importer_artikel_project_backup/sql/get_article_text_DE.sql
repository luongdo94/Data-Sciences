SELECT t.*, m.ArtNr, s.Hauptfarbe, s.Pflegekennzeichnung
FROM (t_Art_Text_DE t 
INNER JOIN t_Art_MegaBase m ON t.ArtNr = m.ArtNr)
INNER JOIN t_Art_Mega_SKU s ON m.ArtNr = s.ArtNr
WHERE m.Marke IN ('Corporate', 'EXCD', 'XO')
AND s.Hauptfarbe IS NOT NULL
AND m.ArtNr IN ({aid_placeholders})