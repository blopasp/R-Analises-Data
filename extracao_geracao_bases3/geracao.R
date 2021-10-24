# Gerando metricas para a analise ##############################################

test <- today()-1

prod<-prod%>%
  mutate(
    #transformando data que está no formato indicado
    DT_OCORR = as.Date(DT_OCORR),
    DT_LIBCAD = as.Date(DT_LIBCAD),
    #venda média por mÊs
    VD_MD_MES = round(VENDA_QT_6M/6,0),
    #QUANTIDADE estoque
    QT_ESTOQUE = ifelse(TP_MOV == "AR",(-QT_MOV), (QT_MOV)),
    #Calculo do PME ENTRADA
    PME_ENTRADA = ifelse((VD_MD_MES == 0 | is.na(VD_MD_MES)), 10000, round(QT_ESTOQUE/VD_MD_MES,0)),
    #Faixa PME ENTRADA
    FAIXAPMEE = ifelse(PME_ENTRADA>12,"2>12", as.character(trunc(PME_ENTRADA))),
    FAIXAPMEE = ifelse(PME_ENTRADA>12,"2>12", ifelse(PME_ENTRADA/10 < 1, paste0("0", trunc(PME_ENTRADA)), as.character(trunc(PME_ENTRADA)))), #as.character(trunc(PME_ENTRADA))),
    #transformando faixa PME ENTRADA como fator
    FAIXAPMEE = as.factor(FAIXAPMEE),
    #criando flag para os produtos com periodo de liberacao maior ou igual a seis meses
    FLAG = ifelse(((as.yearmon(test) - as.yearmon(DT_LIBCAD))*12 )<= 6, "NOVO", "ANTIGO"),
    #Cálculo PME SALDO
    PME_SALDO = ifelse((VD_MD_MES == 0 | is.na(VD_MD_MES)), 10000, round(QT_SALDO/VD_MD_MES,0)),
    #Faixa PME ENTRADA
    FAIXAPMES = ifelse(PME_SALDO > 12,"2>12", as.character(trunc(PME_SALDO))),
    #transformando faixa PME ENTRADA como fator
    FAIXAPMES = as.factor(FAIXAPMES),
    #calculando demanda das lojas
    DEMANDA = ifelse((QT_FILIAIS_ATIVAS*EA) > DEMANDA_LOJAS, QT_FILIAIS_ATIVAS*EA,DEMANDA_LOJAS),
    #calculando excesso que é a quantidade de movimento sobre a demanda
    EXCESSO = (QT_MOV/DEMANDA - 1)
  )
gc()

# incluindo junção de outros campos ############################################

basenf<-nf %>%
  select(DEPO_CD_DEPOSITO, COD_PRODUTO, NECA_DT_JULPEDFOR, NECA_SQ_DTJULPDFOR, NR_NOTA_FISCAL)

rm(nf)
gc()

base<-prod%>%
  left_join(basenf, by = c("CD_DEPOSITO"="DEPO_CD_DEPOSITO", "CD_PRODUTO"="COD_PRODUTO", "KADE_TX_NR_DOCTO"="NR_NOTA_FISCAL"))

#apagando base nf
rm(basenf)
gc()

#gerando a base motivo para lincar com a base selecionando algumas colunas
mot<-motivo %>%
  select(PEFC_DT_JULPEDFOR, PEFC_SQ_DTJULPDFOR, MOTIVO, PRME_CD_PRODUTO, PEFC_CD_DEPOENTREG)

# apagando motivo
rm(motivo)
gc()

#lincando base com motivo
base <-base %>%
  left_join(mot, by = c("CD_DEPOSITO"="PEFC_CD_DEPOENTREG", "CD_PRODUTO"="PRME_CD_PRODUTO", "NECA_DT_JULPEDFOR"="PEFC_DT_JULPEDFOR", "NECA_SQ_DTJULPDFOR"="PEFC_SQ_DTJULPDFOR"))

#apagando base mot
rm(mot)
gc()

forn<-comp %>% select(CD_PRODUTO, FORNECEDOR)

# selecionando por data e inserindo a opção de ser produto especial e embalagem minima
base <-base%>%
  filter(month(DT_OCORR) == month(test) & year(DT_OCORR) == year(test)) %>%
  #lincando com embalagem minima
  left_join(em_min, by = c("CD_DEPOSITO"="DEPO_CD_DEPOSITO", "CD_PRODUTO" = "PRME_CD_PRODUTO"))%>%
  #lincando com r_especial
  left_join(r_especial, by = c("CD_PRODUTO"="PROD"))%>%
  #lincando fornecedor
  left_join(forn, by = c("CD_PRODUTO"="CD_PRODUTO"))

gc()

#Incluindo tag para motivo não encontrado na base
base$MOTIVO<-ifelse(base$MOTIVO == '' | is.na(base$MOTIVO), 'Sem motivo registrado na base',base$MOTIVO)

rm(forn)
gc()

# retirando a descrição de não classificado do nivel 2
cat<-categ

cat<-cat%>%
  filter(
    NIVEL2 != ("NAO CLASSIFICADO")
  )

cat$NIVEL2 <- as.factor(cat$NIVEL2)
#summary(cat$NIVEL2)
gc()
# gerando uma base apenas com produtos que não possuem classificação de 'não classificado'
base<-semi_join(base,cat, by = c("CD_PRODUTO"="CD_PRODUTO"))
#apagando base cat

categ$NIVEL2<-ifelse(categ$NIVEL2=="RX", paste0(categ$NIVEL2, " - ", categ$NIVEL5), categ$NIVEL2)

rm(cat, r_especial, em_min, prod)
i<-12
if(i/10 < 1){
  i<-paste0("0", i)
}else i

format(1:10)
format(1:10, trim = TRUE)
