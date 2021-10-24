# ===== PACOTES =====
library(stringr)
library(dplyr)
library(lubridate)
library(tibble)
library(RODBC)
library(furrr)
library(doParallel)
library(data.table)
library(zoo)

# ==== AJUSTANDO PARALELISMO PARA RODAR BASES ====
nc1 <- detectCores()

c1 <- makeCluster(nc1)
registerDoParallel(c1)

# ==== CONSULTAS ========

# Venda média dia (vmd) ====

baseVmd <- str_c(consulta, pattern = " ")

# Base reversa (consReversa) ====
consultaReversa <- str_c(consulta, pattern = " ")

# Base venda media dia cd (cd_vmd) ====

cd_vmd <- str_c(consulta, pattern = " ")

# Base base cd (cd) ====
cd <- str_c(consulta, pattern = " ")


# Base categoria (consUC) ====
consCateg <- str_c("", pattern = " ")


# importando bases via cosmos ====
con <- odbcDriverConnect('driver={SQL Server};server=cosmos;database=cosmos_v14b')

gc()

# ===== METRICAS REVERSA =====

# === extraindo baseReversa e base venda media dia
vmd <- sqlQuery(con, baseVmd)
baseReversa <- sqlQuery(con, consultaReversa)

gc()

# === Join com base de venda média dia
baseReversa <- baseReversa %>%
  left_join(vmd, by = c("FILIAL" = "Filial", "PRODUTO" = "Produto"))

rm(vmd)

# === Criando parametros da baseReversa
baseReversa <- as.Date(baseReversa$DT_ULTVENDA)

baseReversa$SIT_FAC = as.factor(baseReversa$SIT_FAC)

# === Criando métricas de periodo sem venda e regra para quantidade de faceamento
hoje <- today()

baseReversa <- baseReversa %>%
  mutate(
    PERIODO_SV = ifelse(is.na(DT_ULTVENDA), 10000, as.integer((as.yearmon(hoje) - as.yearmon(DT_ULTVENDA))*12)),
    QT_FACE = ifelse(SITUACAO == 'D', 0, 
                ifelse(QTD_FAC >= EA & SIT_FAC == "A", 
                    QTD_FAC, EA
              )),
    DIF_EA_FAC = ifelse((QT_ESTOQATUAL - QT_FACE) < 0, 0,(QT_ESTOQATUAL - QT_FACE))
  )


# === funcao para calcular excesso das lojas baseano no pme
pme <- function(base, dias){
  hoje <- today()
  
  base <- base %>%
    mutate(
        EXC = ifelse(
        SITUACAO == "D", QT_ESTOQATUAL,
        ifelse(PERIODO_SV > 3, DIF_EA_FAC,
          ifelse(round(
              ifelse(VMD == 0 | is.na(VMD), DIF_EA_FAC, 
                ifelse((QT_ESTOQATUAL - VMD*dias) < 0, 0,
                  QT_ESTOQATUAL - VMD*dias
          ))) < DIF_EA_FAC, 
          round(
              ifelse(VMD == 0 | is.na(VMD), DIF_EA_FAC, 
                ifelse((QT_ESTOQATUAL - VMD*dias) < 0, 0,
                  QT_ESTOQATUAL - VMD*dias
          ))), DIF_EA_FAC
        )))
  )
  return(base$EXC)
}

# === Criando metricas de excesso a partir da funcao pme
baseReversa$EXC_90 <- pme(baseReversa, 90)
baseReversa$EXC_120 <- pme(baseReversa, 120)
baseReversa$EXC_150 <- pme(baseReversa, 150)
baseReversa$EXC_180 <- pme(baseReversa, 180)
baseReversa$EXC_360 <- pme(baseReversa, 360)
baseReversa$EXC_720 <- pme(baseReversa, 720)

# === Criando metricas de valor de excesso
baseReversa <- baseReversa %>%
  mutate(
    VL_90 = EXC_90 * VL_CMPG,
    VL_120 = EXC_120 * VL_CMPG,
    VL_150 = EXC_150 * VL_CMPG,
    VL_180 = EXC_180 * VL_CMPG,
    VL_360 = EXC_360 * VL_CMPG,
    VL_720 = EXC_720 * VL_CMPG,
  )


#write.table(vmd, "baseVMD.txt", row.names = F)

# === Extraindo baseReversa
write.table(baseReversa, "baseReversa-COMPLETA.txt", row.names = F)

rm(baseReversa)


# ===== METRICAS CD =====

# === Extraindo  bases do CD
vmdcd <- sqlQuery(con, cd_vmd)
baseCD <- sqlQuery(con, cd)

# Criando métricas para a baseCD
baseCD <- left_join(baseCD, vmdcd, by = c("CD_DEP"="CD", "CD_PRODUTO"="Produto"))

pmecd <- function(base, dias){
  PME <- as.integer(base$QT_ESTQATUAL/(base$VMD * dias))
  return(PME)
}

baseCD$CAPACIDADE_30 <- round(baseCD$VMD * 30)
baseCD$CAPACIDADE_60 <- round(baseCD$VMD * 60)
baseCD$CAPACIDADE_90 <- round(baseCD$VMD * 90)
baseCD$CAPACIDADE_120 <- round(baseCD$VMD * 120)
baseCD$CAPACIDADE_150 <- round(baseCD$VMD * 150)
baseCD$CAPACIDADE_180 <- round(baseCD$VMD * 180)
baseCD$CAPACIDADE_360 <- round(baseCD$VMD * 360)
baseCD$CAPACIDADE_720 <- round(baseCD$VMD * 720)

write.table(baseCD, "baseCD.txt", row.names = F)


# ===== STOP CLUSTER =====
stopCluster(c1)
