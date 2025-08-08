SELECT m.ArtBasis AS aid, m.Produktgruppe as product_group, m.Marke as Marke, 
       m.Grammatur as Grammatur, m.Artikel_Partner as Artikel_Partner, 
       m.ArtSort as ArtSort, m.Materialart as Materialart, 
       m.Zusammensetzung as Zusammensetzung, m.Gender as Gender, 
       f.flag_workwear as workwear, f.flag_veredelung as veredelung, 
       f.flag_discharge as discharge, f.flag_dtg as dtg, 
       f.flag_dyoj as dyoj, f.flag_dyop as dyop, 
       f.flag_flock as flock, f.flag_siebdruck as siebdruck, 
       f.flag_stick as stick, f.flag_sublimation as sublimation, 
       f.flag_transfer as transfer, f.flag_premium as premium, 
       f.flag_extras as extras, f.flag_outdoor as outdoor, 
       f.flag_plussize as oversize, f.isNoLabel as label, 
       f.isErw as erw 
FROM [t_Art_MegaBase] m 
INNER JOIN [t_Art_Flags] f ON m.ArtNr = f.ArtNr 
WHERE m.Marke IN ('Corporate', 'EXCD', 'XO')
AND m.ArtBasis IN ({aid_placeholders})
