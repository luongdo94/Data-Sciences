SELECT 
                sku.ArtikelCode AS aid,
                0 as company,
                t.SuchText as keyword_list,
                'de' as language,
                ',' as separator
            FROM (t_Art_MegaBase m
            INNER JOIN t_Art_Mega_SKU sku ON m.ArtikelNeu = sku.ArtikelNeu)
            INNER JOIN t_Art_Text_DE t ON sku.ArtikelNeu = t.ArtikelNeu
            WHERE m.Marke IN ('Corporate', 'EXCD', 'XO')
            AND sku.ArtikelCode IN ({aid_placeholders})
            AND sku.Hauptfarbe is not null
            AND sku.FarbeNeu NOT LIKE '%/%'
