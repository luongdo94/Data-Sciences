SELECT t.*, l.LandKfz, l.CodeLang
from tOrderAdr t
inner join tLand l on t.LandCode = l.LandCode
