SELECT t.*, t2.UStID, t3.CodeLang, t3.LandKfz 
FROM ((tAdressen AS t
INNER JOIN tAdrUstId AS t2 ON t.AdrID = t2.AdrID)
INNER JOIN tLand AS t3 ON t.LandCode = t3.LandCode)
