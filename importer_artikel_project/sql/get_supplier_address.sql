SELECT DISTINCT t.*, m.AdrId AS SupplierID, l.LandKfz, l.CodeLang
FROM (tOrderAdr AS t 
INNER JOIN tLand AS l ON t.LandCode = l.LandCode) 
INNER JOIN t_OrderMain AS m ON t.AdrId = m.AdrId;
