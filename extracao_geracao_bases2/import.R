library(RODBC)
library(dplyr)
library(lubridate)
library(writexl)
library(stringr)

# ====== CONSULTAS =======
# ==== Consulta base (consultBase)====
consultBase <- str_c(consulta, pattern = " ")

# === Consulta base VMD (baseVmd) ====
baseVmd <- str_c(consulta, pattern = " ")

# ==== Consulta das perda no kardex atual (venc) ====
venc <- str_c(consulta, pattern = " ")

# ==== Consulta das perda no kardex backup (venc_c) ====
venc_c <- str_c(consulta, pattern = " ")

# ====== FUNCOES =======

# Função para extrair base de perda (avaria ou vencidos) ou da tabela kardex atual ou dos backups
edit_consulta <- function(cons, cons_c, data){
  
  anomes <- format(data,'%Y%m')
  anomes <- as.character(anomes)
  mes <- month(data)
  
  tryCatch(
    #dados do backup (cosmossdw)
    expr = {
      
      consult_c <- str_c(cons_c)
      consult_c <- str_glue(consult_c, pattern = " ")
      consult_c <- str_c(consult_c)
      
      print(consult_c)
      
      con <- odbcDriverConnect('driver={SQL Server} ;server=cosmos;database=cosmos_v14b')
      
      bs <- sqlQuery(con, consult_c)
      bs <- unique(bs)
      
      length(bs)
      
      if(length(bs)==2){
        bs$DataOcorrencia <- NULL
      }
      message(str_glue("base {anomes} extraida com sucesso!"))
      
      return(bs)
      
    },
    warning = function(w){
      #dados do cosmos_v14b atual
      dt <- seq(ymd(data), by = "month", length.out = 2)
      data1<-dt[1]
      data2<-dt[2]-1
      
      rm(dt) 
      
      consult <- str_glue(cons, pattern = " ")
      consult <- str_c(consult)
      
      print(consult)
      
      bs <- sqlQuery(con, consult)
      bs <- unique(bs)
      
      #bs$DataOcorrencia <- NULL
      
      message(str_glue("base {anomes} extraida com sucesso!"))
      
      return(bs)
    }
  )
  
}

# Função para unir dados de perda (avaria ou vencidos)
basePerda <- function(dti = today(), lg, cons, cons_c){
  
  
  dti <- as.Date(dti)
  
  data <- paste0(year(dti), "-0", month(dti)-1, "-", "01")
  
  acumulado<-tibble()
  
  seq_mes <- seq(ymd(data), length = lg, by = "-1 month")
  print(seq_mes)
  
  for(k in 1:length(seq_mes)){
    dt<-seq_mes[k]
    
    base <- edit_consulta(cons, cons_c, dt)
    
    gc()
    
    acumulado <- union_all(acumulado, base)
    
    rm(base)
    gc()
  }
 
  acumulado <- acumulado %>%
    group_by(
      Produto,
      Filial,
    )%>%
    summarise(
      ValorPerda = sum(ValorPerda),
      NumeroOcorrencia = sum(NumeroOcorrencia)
    )%>%
    filter(
      ValorPerda > 0,
      NumeroOcorrencia > 0
    )

  return(acumulado)
}

# Funcao PME 
pme <- function(base, dias){
  PME <- as.integer(base$QTDEstqAtual/(base$VMD * dias))
  return(PME)
}

# ====== GERACAO ======

con = odbcDriverConnect('driver={SQL SERVER};server=cosmos;database=cosmos_v14b')

# extraindo bases
base <- sqlQuery(con, consultBase)

baseVMD <- sqlQuery(con, baseVmd)

base <- base %>% 
  left_join(baseVMD, by = c("Filial", "Produto"))

 rm(baseVMD)

# extraindo base perdas
perda <- basePerda(lg = 12, cons = venc, cons_c = venc_c)

base <- base %>% 
  left_join(perda, by = c("Filial", "Produto"))

rm(perda)

# ====== METRICAS ======

base$PME <- pme(base, 30)

base <- base %>% 
  mutate(
    FaixaPME = ifelse(is.na(PME), 'SEM VENDA',
      ifelse(
        PME > 12,"2>12", 
        ifelse(
          PME/10 < 1, paste0("0", trunc(PME)), 
          as.character(trunc(PME))
        )
      )
    )
  )

base <- unique(base)

write.table(base, "baseFracionamento.txt", row.names = F)
