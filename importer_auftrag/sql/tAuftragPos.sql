SELECT 
    p.AuftragID,
    p.PosID,
    p.Menge1 + p.Menge2 + p.Menge3 + p.Menge4 + p.Menge5 + p.Menge6 + p.Menge7 + p.Menge8 + p.Menge9 AS quantity,
    a.erfaßt_am as login,
    'Düsseldorf' as factory  
FROM 
    tAuftragPos p
INNER JOIN
    tAuftrag a ON a.AuftragID = p.AuftragID