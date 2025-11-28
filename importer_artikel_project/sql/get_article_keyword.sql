SELECT m.ArtikelNeu AS aid, t.SuchText as keyword 
FROM [t_Art_MegaBase] m 
INNER JOIN t_Art_Text_DE t ON m.ArtikelNeu = t.ArtikelNeu 
WHERE m.Marke IN ('Corporate', 'EXCD', 'XO')
AND m.ArtikelNeu IN ({aid_placeholders})
