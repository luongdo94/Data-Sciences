SELECT o.OrderNr_Lang, o.POCode, AdrId, o.OSDate , o.OrgDatum, k.[erfaßt_von], k.[Name]
FROM [t_OrderMain] AS o
LEFT JOIN [kontrakt_user] AS k ON o.[erfaßt_von] = k.[erfaßt_von]
WHERE o.[AdrId] <> 15;

