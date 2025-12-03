SELECT t.*, m.ArtikelNeu, s.Hauptfarbe, s.Pflegekennzeichnung
FROM (t_Art_Text_EN t 
INNER JOIN t_Art_MegaBase m ON t.ArtikelNeu = m.ArtikelNeu)
INNER JOIN t_Art_Mega_SKU s ON m.ArtikelNeu = s.ArtikelNeu
WHERE m.Marke IN ('Corporate', 'EXCD', 'XO')
AND s.Hauptfarbe IS NOT NULL
AND m.ArtikelNeu IN ({aid_placeholders})
