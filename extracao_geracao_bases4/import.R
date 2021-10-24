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
library(writexl)

# ==== AJUSTANDO PARALELISMO PARA RODAR BASES ====
nc1 <- detectCores()

c1 <- makeCluster(nc1)
registerDoParallel(c1)

periodo <- format(today(), "%Y%m")

cam = ''

pasta <- paste0(cam,'/', periodo)

if(length(list.dirs(pasta)) == 0){
  dir.create(pasta)
} 

# ==== CONSULTAS ========
# Venda média dia (vmd) ====
baseVmd <- str_c("")

# Base reversa (consReversa) ====
consultaReversa <- str_c("")

# Base venda media dia cd (cd_vmd) ====
cd_vmd <- str_c("")

# Base base cd (cd) ====
cd <- str_c("")


# Base categoria (consUC) ====
consCateg <- str_c("")


# Importando bases via cosmos ====
message("Processo iniciado")

con <- odbcDriverConnect('driver={SQL Server};server="";database=""')

# ===== FUNÇÕES =====

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

# === FUNÇÃO PARA GERAR VALORES DE EXCESSO EM RELAÇÃO À OPORTUNIDADE DOS CD'S
oport <- function(base, coluna){
  base[[paste0(coluna, '_OP30')]] <- ifelse(base[[coluna]] > baseTableau$Capacidade30, baseTableau$Capacidade30, baseTableau[[coluna]])
  base[[paste0(coluna, '_OP90')]] <- ifelse(base[[coluna]] > baseTableau$Capacidade90, baseTableau$Capacidade90, baseTableau[[coluna]])
  base[[paste0(coluna, '_OP180')]] <- ifelse(base[[coluna]] > baseTableau$Capacidade180, baseTableau$Capacidade180, baseTableau[[coluna]])
  base[[paste0(coluna, '_OP360')]] <- ifelse(base[[coluna]] > baseTableau$Capacidade360, baseTableau$Capacidade360, baseTableau[[coluna]])
  
  return(base)
}

# === FUNÇÃO DE PME DE ACORDO COM O CD
pmecd <- function(base, dias){
  PME <- as.integer(base$QT_ESTQATUAL/(base$VMD * dias))
  return(PME)
}


# ===== METRICAS REVERSA =====

# === extraindo baseReversa e base venda media dia
vmd <- sqlQuery(con, baseVmd)
message("Base venda media loja extraida com sucesso")

# === extraindo base principal
baseReversa <- sqlQuery(con, consultaReversa)
message("Base reversa extraida com sucesso")

gc()

# === Join com base de venda média dia
baseReversa <- baseReversa %>%
  left_join(vmd, by = c("FILIAL" = "Filial", "PRODUTO" = "Produto"))

rm(vmd)

# === Alterando formato das colunas
baseReversa$DT_ULTVENDA <- as.Date(baseReversa$DT_ULTVENDA)
baseReversa$SIT_FAC = as.factor(baseReversa$SIT_FAC)

# ==== Criando métricas de periodo sem venda e regra para quantidade de faceamento ====
hoje <- today()

# === Criando métricas PERIODO_SV, QT_FACE e DIF_EA_FAC
baseReversa <- baseReversa %>%
  mutate(
    PERIODO_SV = ifelse(is.na(DT_ULTVENDA), 10000, as.integer((as.yearmon(hoje) - as.yearmon(DT_ULTVENDA))*12)),
    QT_FACE = ifelse(SITUACAO == 'D', 0, 
                ifelse(QTD_FAC >= EA & SIT_FAC == "A", 
                    QTD_FAC, EA
              )),
    DIF_EA_FAC = ifelse((QT_ESTOQATUAL - QT_FACE) < 0, 0,(QT_ESTOQATUAL - QT_FACE))
  )

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

message("Base reversa com parametros e metricas ajustadas")
#write.table(vmd, "baseVMD.txt", row.names = F)

# ==== Exportando baseReversa ====
if(paste0(pasta, '/', 'baseReversa-COMPLETA.txt') %in% list.files(pasta)){
  message("Já adicionado")
} else {
  write.table(baseReversa, paste0(pasta, '/baseReversa-COMPLETA.txt'), row.names = F)
}

message("Base reversa exportada com sucesso")

# Extruturando parametros para dados do Tableau de acordo com a baseReversa ====
base <- baseReversa %>% 
  group_by(
    PRODUTO,
    DEPOSITO
  ) %>% 
  summarise(
    Valor90 = sum(VL_90, na.rm = T),
    Valor120 = sum(VL_120, na.rm = T),
    Valor150 = sum(VL_150, na.rm = T),
    Valor180 = sum(VL_180, na.rm = T),
    Valor360 = sum(VL_360, na.rm = T),
    Valor720 = sum(VL_720, na.rm = T),
    Excesso90 = sum(EXC_90, na.rm = T),
    Excesso120 = sum(EXC_120, na.rm = T),
    Excesso150 = sum(EXC_150, na.rm = T),
    Excesso180 = sum(EXC_180, na.rm = T),
    Excesso360 = sum(EXC_360, na.rm = T),
    Excesso720 = sum(EXC_720, na.rm = T)
  ) %>% 
  arrange(
    desc(PRODUTO),
    DEPOSITO
  )

rm(baseReversa)
message("paranetros iniciais da base tableau ajustada")

# ===== METRICAS CD =====

# === Extraindo  bases do CD
vmdcd <- sqlQuery(con, cd_vmd)
message("Base venda media cd extraida com sucesso")

baseCD <- sqlQuery(con, cd)

message("Base capacidade CD extraida com sucesso")

# Criando métricas para a baseCD
baseCD <- left_join(baseCD, vmdcd, by = c("CD_DEP"="CD", "CD_PRODUTO"="Produto"))

# CRIANDO MÉTRICAS PARA AVALIAR A CAPACIDADE DOS CDS
baseCD$CAPACIDADE_30 <- round(baseCD$VMD * 30)
baseCD$CAPACIDADE_60 <- round(baseCD$VMD * 60)
baseCD$CAPACIDADE_90 <- round(baseCD$VMD * 90)
baseCD$CAPACIDADE_120 <- round(baseCD$VMD * 120)
baseCD$CAPACIDADE_150 <- round(baseCD$VMD * 150)
baseCD$CAPACIDADE_180 <- round(baseCD$VMD * 180)
baseCD$CAPACIDADE_360 <- round(baseCD$VMD * 360)
baseCD$CAPACIDADE_720 <- round(baseCD$VMD * 720)

# ==== Exportando baseCD ====
if(paste0(pasta, "/baseCD.txt") %in% list.files(pasta)){
  message("Já adicionado")
} else {
  write.table(baseCD, paste0(pasta, "/baseCD.txt"), row.names = F)
}

# ==== MANIPULANDO A baseCD PARA EXPORTAR PARA O TABLEAU ====
base1 <- baseCD %>% 
  group_by(
    CD_DEP,
    CD_PRODUTO
  ) %>% 
  summarise(
    Capacidade30 = sum(CAPACIDADE_30, na.rm = T),
    Capacidade90 = sum(CAPACIDADE_90, na.rm = T),
    Capacidade180 = sum(CAPACIDADE_180, na.rm = T),
    Capacidade360 = sum(CAPACIDADE_360, na.rm = T)
  ) %>% 
  arrange(
    desc(CD_PRODUTO),
    CD_DEP
  )

message("Parametros intermediarios para a base tableau realizados com sucesso")

rm(baseCD)
gc()

# Estruturando base Tableau ====
baseTableau <- left_join(base, base1, by = c("PRODUTO"="CD_PRODUTO", "DEPOSITO"="CD_DEP"))

rm(base, base1)
gc()

# APLICANDO FUNCAO DE OPORTUNIDADE DOS CD'S
baseTableau <- oport(baseTableau, 'Excesso90')
baseTableau <- oport(baseTableau, 'Excesso120')
baseTableau <- oport(baseTableau, 'Excesso150')
baseTableau <- oport(baseTableau, 'Excesso180')
baseTableau <- oport(baseTableau, 'Excesso360')
baseTableau <- oport(baseTableau, 'Excesso720')

# Lincando com categorias ====
con <- odbcDriverConnect('driver={SQL Server};server=cosmos;database=cosmos_v14b')

gc()

# Extraindo base categoria
categ <- sqlQuery(con, consCateg)
message("Base Categoria Extraida com sucesso")

# Selecionando campos da base de categoria
categ <- categ %>% 
  select(CD_PRODUTO, PROD_DESC = ProdutoDescricao , Nivel4, Termo, EMBALAGEM)

# lincando base principal com base categorias
baseTableau <- left_join(baseTableau, categ, by = c("PRODUTO"="CD_PRODUTO"))

# negando função %in%
`%notin%` <- Negate(`%in%`)
ngrepl <- Negate(grepl) 

# Aplicando Filtros para categorias não contempladas pela reversa
baseTableau <- baseTableau %>% 
  filter(
    ngrepl("TERMO", Termo),
    ngrepl("ABSORVENTE", Nivel4),
    ngrepl("ROUPA", Nivel4),
    ngrepl("FRALDA", Nivel4),
    ngrepl("EV", EMBALAGEM)
  ) %>% 
  select(-Nivel4, -Termo, -EMBALAGEM)
message("Base Tableau pronta para exportar com métricas e filtros ja realizados")

periodo <- today()

baseTableau$Atualizacao <- periodo

rm(categ)
gc()
# ==== Exportando base Tableau ====
periodo <- format(periodo, "%Y%m")

baseTableau_b <- paste0(pasta, "/DadosReversa.xlsx")

if(paste0(pasta, "/DadosReversa.xlsx") %in% list.files(pasta)){
  message("Já adicionado")
} else {
  write_xlsx(baseTableau, baseTableau_b)
}

message("Base tableau exportada com sucesso")

gc()

# ===== STOP CLUSTER =====
stopCluster(c1)
rm(c1)
