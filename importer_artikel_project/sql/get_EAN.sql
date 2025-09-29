Select m.Verpackungseinheit, sku.ArtikelCode, ean.EAN13, ean.QtyId
from(t_Art_Mega_SKU sku
                INNER JOIN t_Art_MegaBase m ON sku.ArtNr = m.ArtNr)
                inner join tEANCodes ean ON sku.ArtNr=ean.ArtNr and sku.SizeId=ean.SizeId and sku.FarbCode=ean.Farbe
            WHERE
                m.Marke IN ('Corporate', 'EXCD', 'XO')
                AND sku.Hauptfarbe is not null
                AND ean.EAN13 IN ({aid_placeholders})

               