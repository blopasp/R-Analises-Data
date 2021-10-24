library(dplyr)
library(stringr)
library(lubridate)
library(RODBC)
library(zoo)

con <- RODBC::odbcDriverConnect('driver={SQL Server} ;server=cosmos;database=cosmos_v14b')

# Consultas ####

# Venda Media dia ====
vmd <- str_c("consulta", pattern = " ") 

#consulta base deposito ====

dep <- str_c("consulta", pattern = " ")

# alternativa pme ====
cd_pme <- str_c("consulta", pattern = " ")


# Extração ####

base_vmd <- sqlQuery(con, vmd)
base_dep <- sqlQuery(con, dep)

head(base_dep)
head(base_vmd)


between()
typeof(base_dep$DT_ULTCOMPRA)

base_dep$DT_ULTCOMPRA <- as.Date(base_dep$DT_ULTCOMPRA)

hoje <- today()

base <- base_dep %>%
  left_join(base_vmd, by = c("CD_DEP"="CD", "CD_PRODUTO"="CODPROD"))%>%
  mutate(
    PME = (QT_ESTQATUAL/DMDDIA)*30,
    AGING = round((as.yearmon(hoje) - as.yearmon(DT_ULTCOMPRA))*12,0),
    #faixa pme
    FAIXA_PME = ifelse(
      is.na(PME), "SV",
      ifelse(
        between(PME, 0,3), "0 - 3",
        ifelse(
          between(PME, 3,6), "3 - 6",
          ifelse(
            between(PME, 6, 9), "6 - 9",
            ifelse(
              between(PME, 9, 12), "9 - 12", "> 12"
             )
          )
        )
      )
    ),
    FAIXA_AGING = ifelse(
      is.na(AGING), "NaoEnc",
      ifelse(
        between(AGING, 0,3), "0 - 3",
        ifelse(
          between(AGING, 4,6), "3 - 6",
          ifelse(
            between(AGING, 7, 9), "6 - 9",
            ifelse(
              between(AGING, 9, 12), "10 - 12", "> 12"
            )
          )
        )
      )
    )
  )

write.csv2(base, local, row.names = F)
