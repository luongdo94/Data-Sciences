Select p.OrderId, p.ArtikelCode, p.OPreis, p.Menge, m.OrderNr_Lang, m.erfaßt_am from t_OrderPos p inner join t_OrderMain m
on  p.OrderId=m.OrderId
where m.AdrId <> 15
