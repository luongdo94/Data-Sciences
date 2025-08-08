SELECT 
                sku.ArtikelCode AS aid,
                0 as company,
                t.SuchText as keyword_list,
                'de' as language,
                ',' as seperator
            FROM (t_Art_MegaBase m
            INNER JOIN t_Art_Mega_SKU sku ON m.ArtNr = sku.ArtNr)
            INNER JOIN t_Art_Text_DE t ON sku.ArtNr = t.ArtNr
            WHERE m.Marke IN ('Corporate', 'EXCD', 'XO')
            AND sku.ArtikelCode IN ({aid_placeholders})
            AND sku.Hauptfarbe is not null
