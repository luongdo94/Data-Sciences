SELECT ArtBasis AS aid, Artikel_Partner as aid_assigned, 
       Artikel_Alternativen as aid_alternativen 
FROM [t_Art_MegaBase]
WHERE Marke IN ('Corporate', 'EXCD', 'XO')
AND ArtBasis IN ({aid_placeholders})
