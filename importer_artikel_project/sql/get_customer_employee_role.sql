SELECT
    p.PAnrede,
    p.AdrID,
    p.PAdrID,
    p.PVorName,
    p.PName,
    p.PBereich,
    t.KNummer,
    t.Name1,
    land.CodeLang
FROM
    (
        tAdressen AS t
        INNER JOIN tAdrPartner AS p ON t.AdrID = p.AdrID
    )
    INNER JOIN tLand AS land ON t.LandCode = land.LandCode;
