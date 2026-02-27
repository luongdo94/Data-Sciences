SELECT DISTINCT 
    m.AdrId AS SupplierID, 
    t.AdrName1 AS Name1, 
    t.AdrName2 AS Name2, 
    t.AdrTel AS Telefon, 
    t.AdrFax AS Fax, 
    l.LandKfz, l.CodeLang
FROM (tOrderAdr AS t 
INNER JOIN tLand AS l ON t.LandCode = l.LandCode) 
INNER JOIN t_OrderMain AS m ON t.AdrId = m.AdrId;
