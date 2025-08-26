SELECT 
                sku.ArtikelCode AS aid,
                m.ArtBasis AS basis,
                m.ArtNr AS ArtNr,
                m.Ursprungsland,
                t.ArtBem AS name,
                sku.Größe AS Größe,
                sku.Größenspiegel AS Größenspiegel,
                sku.Hauptfarbe AS Farbgruppe,
                sku.FarbeNeu AS Farbe,
                sku.isColorCombination AS zweifarbig,
                sku.Karton_Länge as Verpackungslänge,
                sku.Karton_Breite as Verpackungsbreite,
                sku.Karton_Höhe as Verpackungshoehe,
                sku.Produktgewicht as Produktgewicht,
                sku.WarenNr as WarenNr,
                sku.isColorMelange as ColorMelange,
                sku.VZTA_gültig_bis as [VZTA aktiv bis],
                sku.VZTA_gültig_von as [VZTA aktiv von],
                sku.ArtSort AS sku_ArtSort,
                m.Materialart AS Fabric_Herstellung,
                m.Produktgruppe AS product_group,
                m.Marke AS Marke,
                m.Grammatur AS Grammatur,
                m.Artikel_Partner AS Artikel_Partner,
                m.Zusammensetzung AS Zusammensetzung,
                m.Gender AS Gender,
                f.flag_workwear AS workwear,
                f.flag_veredelung AS veredelung,
                f.flag_discharge AS discharge,
                f.flag_dtg AS dtg,
                f.flag_dyoj AS dyoj,
                f.flag_dyop AS dyop,
                f.flag_flock AS flock,
                f.flag_siebdruck AS siebdruck,
                f.flag_stick AS stick,
                f.flag_sublimation AS sublimation,
                f.flag_transfer AS transfer,
                f.flag_premium AS premium,
                f.flag_extras AS extras,
                f.flag_outdoor AS outdoor,
                f.flag_plussize AS oversize,
                f.isNoLabel AS label,
                f.isErw AS erw,
                t.WebVEText as Oeko_MadeInGreen
            FROM 
                ((t_Art_Mega_SKU sku
                INNER JOIN t_Art_Text_DE t ON sku.ArtNr = t.ArtNr)
                INNER JOIN t_Art_MegaBase m ON sku.ArtNr = m.ArtNr)
                INNER JOIN t_Art_Flags f ON m.ArtNr = f.ArtNr
            WHERE 
                m.Marke IN ('Corporate', 'EXCD', 'XO')
                AND sku.ArtikelCode IN ({aid_placeholders})
                AND sku.Hauptfarbe is not null