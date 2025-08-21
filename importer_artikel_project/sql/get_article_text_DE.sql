SELECT t.* FROM t_Art_Text_DE t inner join t_Art_MegaBase m ON t.ArtNr = m.ArtNr
WHERE m.ArtBasis IN ({aid_placeholders})
and m.Marke IN ('Corporate', 'EXCD', 'XO')