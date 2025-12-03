SELECT o.OrderNr as OrderNr_Lang, o.POCode, AdrId, o.OSDate , o.OrgDatum, k.[erfaßt_von], k.[Name], o.OrderNr_Lang as OrderNr
FROM [t_OrderMain] AS o
LEFT JOIN [kontrakt_user] AS k ON o.[erfaßt_von] = k.[erfaßt_von]
WHERE o.[AdrId] = 15