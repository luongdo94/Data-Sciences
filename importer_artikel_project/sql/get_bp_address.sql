SELECT t.*, ust.UStID, t3.CodeLang, t3.LandKfz,
    (
        SELECT
            TOP 1 isRechnungPDF
        FROM
            tAdrPartner
        WHERE
            AdrID = t.AdrID
    ) AS isRechnungPDF
FROM ((tAdressen AS t
INNER JOIN tAdrUstId AS ust ON t.AdrID = ust.AdrID)
INNER JOIN tLand AS t3 ON t.LandCode = t3.LandCode)
