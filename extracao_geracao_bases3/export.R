#gerando base de lan?amentos
#summary(categ)
ca<-categ%>%
  select(CD_PRODUTO, NIVEL2)

base_lanc<- base %>%
  filter(
    FLAG == 'NOVO',
    EXCESSO >= 0.3, 
    PME_SALDO > 8
  )%>%
  #,month(KADE_DH_OCORR) == month(dt) & year(KADE_DH_OCORR) == year(dt)
  select(-FLAG, -EXCESSO)%>%
  left_join(ca,by="CD_PRODUTO")

gc()

#Renomiando colunas da base lancamento
names(base_lanc)<-c("CD_DEPOSITO","CD_PRODUTO","DESCRICAO","TP_MOV","DT_OCORRENCIA","QT_MOVIMENTO","VL_CMPG","VL_ENTRADA","QT_SALDO","QT_SALDO_ANT","DT_LIBCAD","QT_FILIAIS_ATIVAS","VENDA_QT_6M","ESTOQUE_ALVO","DEMANDA_LOJAS","KADE_TX_NR_DOCTO","VENDA_MEDIA_MES","QT_ESTOQUE","PME_ENTRADA", "FAIXA_PMEE", "PME_SALDO", "FAIXA_PMES","DEMANDA", "NECA_DT_JULPEDFOR","NECA_SQ_DTJULPDFOR", "MOTIVO", "QT_EMBALAGEM_MIN", "TP_RECEBIMENTO", "FORNECEDOR", "CATEGORIA (NIVEL2)")

names(base_lanc)

#gerando base produtos regulares
base_reg<- base %>%
  filter(
    FLAG != 'NOVO',
    PME_SALDO > 8    
  )%>%
  #,month(KADE_DH_OCORR) == month(dt) & year(KADE_DH_OCORR) == year(dt)
  select(-FLAG, -EXCESSO)%>%
  left_join(ca,by="CD_PRODUTO")

gc()

sum(base_reg$VL_ENTRADA)
sum(base_lanc$VL_ENTRADA)

#Renomiando colunas da base lancamento
names(base_reg)<-c("CD_DEPOSITO","CD_PRODUTO","DESCRICAO","TP_MOV","DT_OCORRENCIA","QT_MOVIMENTO","VL_CMPG","VL_ENTRADA","QT_SALDO","QT_SALDO_ANT","DT_LIBCAD","QT_FILIAIS_ATIVAS","VENDA_QT_6M","ESTOQUE_ALVO","DEMANDA_LOJAS","KADE_TX_NR_DOCTO","VENDA_MEDIA_MES","QT_ESTOQUE","PME_ENTRADA", "FAIXA_PMEE", "PME_SALDO", "FAIXA_PMES","DEMANDA", "NECA_DT_JULPEDFOR","NECA_SQ_DTJULPDFOR", "MOTIVO", "QT_EMBALAGEM_MIN", "TP_RECEBIMENTO", "FORNECEDOR", "CATEGORIA (NIVEL2)")

rm(ca)

setwd("C:/Estudos/ENTRADA CD/bases")

##criando caminho para diretório
dt<-str_c(format(today(), '%Y%m'))

cam<-paste0("C:/Estudos/Entrada CD/bases backup/",dt)
cam<-as.character(cam)

#criando função para criar idicador de previa ou nao
#e criar um diretorio
if(day(today()) > 10 & day(today())<=16){
  ind<-paste0("previa_", dt,".xlsx")
} else{
  ind<-paste0(dt,".xlsx")
  dir.create(cam)
}

#caminho da base backup
baseb<-paste0(cam,"/base_", ind)

#caminho das bases filtradas por email
base_regb<-paste0(cam,"/base_regular_",ind)
base_lancb<-paste0(cam,"/base_lancamento_",ind)

rm(dt, cam, ind)
gc()

#exportando bases
writexl::write_xlsx(base, "base.xlsx")
writexl::write_xlsx(base_reg, "base_regular.xlsx")
writexl::write_xlsx(base_lanc, "base_lancamento.xlsx")
write.csv2(categ,"categoria.csv", row.names = FALSE)

#bases backup
writexl::write_xlsx(base, baseb)
writexl::write_xlsx(base_reg, base_regb)
writexl::write_xlsx(base_lanc, base_lancb)

gc()

rm(base, base_lanc, base_reg, categ, comp,con, base_lancb, base_regb, baseb)

