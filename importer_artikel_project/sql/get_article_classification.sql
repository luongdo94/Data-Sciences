SELECT 
    m.ArtBasis as aid,
    m.Produktgruppe as product_group,
    m.Marke,
    m.Artikel_Partner,
    m.Grammatur,
    m.ArtSort,
    m.Materialart,
    m.Zusammensetzung,
    m.Gender,
    f.flag_workwear as workwear,
    f.flag_veredelung as veredelung,
    f.flag_discharge as discharge,
    f.flag_dtg as dtg,
    f.flag_dyoj as dyoj,
    f.flag_dyop as dyop,
    f.flag_flock as flock,
    f.flag_siebdruck as siebdruck,
    f.flag_stick as stick,
    f.flag_sublimation as sublimation,
    f.flag_transfer as transfer,
    f.flag_premium as premium,
    f.flag_extras as extras,
    f.flag_outdoor as outdoor,
    f.flag_plussize as oversize, 
    f.isErw as erw, 
    f.flag_60grad as Grad_60, 
    f.flag_nolabel as No_Label,
    f.flag_specialoffer as specialoffer
FROM [t_Art_MegaBase] m 
INNER JOIN [t_Art_Flags] f ON m.ArtikelNeu = f.ArtikelNeu
WHERE m.Marke IN ('Corporate', 'EXCD', 'XO')
AND m.ArtBasis IN ({aid_placeholders})
