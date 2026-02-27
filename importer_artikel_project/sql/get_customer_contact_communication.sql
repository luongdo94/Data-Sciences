SELECT
    p.PAnrede,
    p.AdrID,
    p.PAdrID,
    p.PVorName,
    p.PName,
    p.PUserPw,
    p.WebUserName,
    p.PBereich,
    p.DWahl,
    p.EMail,
    t.KNummer,
    t.Name1,
    t.Name2,
    land.CodeLang,
    land.LandKfz
FROM
    (
        tAdressen AS t
        INNER JOIN tAdrPartner AS p ON t.AdrID = p.AdrID
    )
    INNER JOIN tLand AS land ON t.LandCode = land.LandCode;
