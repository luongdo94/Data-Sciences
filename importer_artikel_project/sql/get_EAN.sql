Select m.Verpackungseinheit, sku.EAN, sku.ArtikelCode from(t_Art_Mega_SKU sku
                INNER JOIN t_Art_MegaBase m ON sku.ArtNr = m.ArtNr)
            WHERE
                m.Marke IN ('Corporate', 'EXCD', 'XO')
                AND sku.Hauptfarbe is not null
               