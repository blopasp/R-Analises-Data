con <- odbcDriverConnect('driver={SQL Server} ;server=cosmos;database=cosmos_v14b')

setwd("C:/Estudos/ENTRADA CD/bases")

prod <- sqlQuery(con, base_produtos)

categ <- sqlQuery(con, categoria)

nf <- sqlQuery(con, notas_fiscais)

motivo<-sqlQuery(con, base_motivo)

em_min <- sqlQuery(con, base_minima)

comp <- sqlQuery(con, base_comp)

r_especial<-read_excel("base_recebimento_especial.xlsx")
colnames(r_especial)[1] <- "PROD"

