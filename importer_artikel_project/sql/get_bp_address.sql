SELECT t.*, ust.UStID, t3.CodeLang, t3.LandKfz 
FROM ((tAdressen AS t
INNER JOIN tAdrUstId AS ust ON t.AdrID = ust.AdrID)
INNER JOIN tLand AS t3 ON t.LandCode = t3.LandCode)
