SELECT 
    tLagerhalle.Bezeichnung AS location, 
    t_Art_MegaBase.ArtikelNeu, 
    t_Art_Mega_SKU.ArtikelCode AS aid, 
    tLagerOrte.StdArtNr, 
    tLagerOrte.StdFarbe, 
    t_Art_Mega_SKU.FarbCode, 
    tLagerOrte.StdSizeID, 
    t_Art_Mega_SKU.FarbeAlt, 
    tLagerOrte.Reihe, 
    tLagerOrte.Regal, 
    tLagerOrte.Palette, 
    tLagerReihen.Halle, 
    tLagerOrte.MaxStk AS quantity, 
    tLagerOrte.MinStk, 
    25 AS refilPoint, 
    -1 AS refilPointIsPercent, 
    tLagerOrte.isPicking, 
    tLagerOrte.isKEausHL 
FROM 
    t_Art_MegaBase 
    INNER JOIN (
        t_Art_Mega_SKU 
        INNER JOIN (
            (tLagerOrte 
            INNER JOIN tLagerReihen ON tLagerOrte.Reihe = tLagerReihen.Reihe)
            INNER JOIN tLagerhalle ON tLagerReihen.Reihe = tLagerhalle.Reihe
        ) ON (t_Art_Mega_SKU.ArtNr = tLagerOrte.StdArtNr 
             AND t_Art_Mega_SKU.FarbCode = tLagerOrte.StdFarbe 
             AND t_Art_Mega_SKU.SizeId = tLagerOrte.StdSizeID)
    ) ON t_Art_MegaBase.ArtikelNeu = t_Art_Mega_SKU.ArtikelNeu 
WHERE 
    tLagerhalle.Bezeichnung = ?
    AND tLagerOrte.isPicking = ?
ORDER BY 
    t_Art_MegaBase.ArtikelNeu, 
    t_Art_Mega_SKU.FarbCode, 
    tLagerOrte.StdSizeID;