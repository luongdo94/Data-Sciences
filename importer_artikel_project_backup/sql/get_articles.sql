SELECT 
    m.ArtBasis as aid, 
    m.Ursprungsland, 
    t.ArtBem as name 
FROM 
    t_Art_MegaBase m 
    INNER JOIN t_Art_Text_DE t ON m.ArtNr = t.ArtNr 
WHERE 
    m.Marke IN ('Corporate', 'EXCD', 'XO')
    AND m.ArtBasis IN ({aid_placeholders})