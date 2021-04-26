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

baseVmd <- str_c("
  DECLARE @DATA01 DATE
  DECLARE @DATA02 DATE
  DECLARE @DATA03 DATE
  DECLARE @DATA04 DATE
  DECLARE @DATA05 DATE
  DECLARE @DATA06 DATE
  DECLARE @DATA07 DATE
  SET @DATA01= DATEADD(DAY,-181,GETDATE())
  SET @DATA02= DATEADD(DAY,30,@DATA01)
  SET @DATA03= DATEADD(DAY,30,@DATA02)
  SET @DATA04= DATEADD(DAY,30,@DATA03)
  SET @DATA05= DATEADD(DAY,30,@DATA04)
  SET @DATA06= DATEADD(DAY,30,@DATA05)
  SET @DATA07= DATEADD(DAY,30,@DATA06)
  
  SELECT
  	T1.Filial
  	,T1.Produto
    ,CAST( SUM(
		CASE 
            WHEN (T1.DT_OCORR_REAL > @DATA01 AND T1.DT_OCORR_REAL <= @DATA02) 
            AND T1.QTDVENDA > 0 THEN T1.QTDVENDA  *0.03
            WHEN (T1.DT_OCORR_REAL > @DATA02 AND T1.DT_OCORR_REAL <= @DATA03) 
            AND T1.QTDVENDA > 0 THEN T1.QTDVENDA*0.07
            WHEN (T1.DT_OCORR_REAL > @DATA03  AND T1.DT_OCORR_REAL <= @DATA04) 
            AND T1.QTDVENDA>0 THEN T1.QTDVENDA*0.1
            WHEN (T1.DT_OCORR_REAL > @DATA04 AND T1.DT_OCORR_REAL <= @DATA05) 
            AND T1.QTDVENDA>0 THEN T1.QTDVENDA*0.2
            WHEN (T1.DT_OCORR_REAL>@DATA05 AND T1.DT_OCORR_REAL<=@DATA06) 
            AND T1.QTDVENDA>0 THEN T1.QTDVENDA*0.27
            WHEN (T1.DT_OCORR_REAL>@DATA06 AND T1.DT_OCORR_REAL<=@DATA07) 
            AND T1.QTDVENDA>0 THEN T1.QTDVENDA*0.33
            ELSE 0 
		END)/30 AS DECIMAL(20,6)) AS VMD

  FROM (
  	SELECT
          KF.KAFI_CD_FILIAL AS Filial
          ,KF.KAFI_CD_PRODUTO AS Produto
          ,KF.KAFI_QT_MOV AS QTDVENDA
          ,CONVERT(DATE, KF.KAFI_DH_OCORRREAL) AS DT_OCORR_REAL
      FROM KARDEX_FILIAL KF WITH(NOLOCK)
              
      INNER JOIN PRODUTO_MESTRE PM WITH(NOLOCK)
      ON KF.KAFI_CD_PRODUTO = PM.PRME_CD_PRODUTO
      INNER JOIN FILIAL FL WITH(NOLOCK)
      ON KF.KAFI_CD_FILIAL = FL.FILI_CD_FILIAL
              
      WHERE 
          KF.KAFI_TP_MOV IN('SV')
          AND CONVERT(DATE, KF.KAFI_DH_OCORRREAL) BETWEEN 
          @DATA01 AND @DATA07
          AND CONVERT(DATE,PM.XXXX_DH_CAD) < @DATA01 AND CONVERT(DATE,FL.XXXX_DH_CAD) < @DATA01
          AND PM.CAPN_CD_CATEGORIA NOT LIKE '3%'
          AND PM.CAPN_CD_CATEGORIA NOT LIKE '1.101.009%'
          AND PM.CAPN_CD_CATEGORIA NOT LIKE '1.102.009%'
          AND PM.CAPN_CD_CATEGORIA NOT LIKE '2.504.001%'
  
  	UNION ALL
  
  	SELECT
  		KFD.KAFI_CD_FILIAL AS FILIAL
  		,KFD.KAFI_CD_PRODUTO AS CODPROD
  		,KFD.KAFI_QT_MOV AS QTDVENDA
  		,KFD.KAFI_DH_OCORRREAL AS DT_OCORR_REAL
  	FROM KARDEX_FILIAL_DIA KFD WITH(NOLOCK)
          
  	INNER JOIN PRODUTO_MESTRE PM WITH(NOLOCK)
  	ON KFD.KAFI_CD_PRODUTO = PM.PRME_CD_PRODUTO
  	INNER JOIN FILIAL FL WITH(NOLOCK)
  	ON KFD.KAFI_CD_FILIAL = FL.FILI_CD_FILIAL
          
  	WHERE 
  		KFD.KAFI_TP_MOV IN('SV')
  		AND CONVERT(DATE,KFD.KAFI_DH_OCORRREAL) BETWEEN @DATA01 AND @DATA07
  		AND CONVERT(DATE,PM.XXXX_DH_CAD) <@DATA01 AND FL.XXXX_DH_CAD <@DATA01
  		AND PM.CAPN_CD_CATEGORIA NOT LIKE '3%'
  		AND PM.CAPN_CD_CATEGORIA NOT LIKE '1.101.009%'
  		AND PM.CAPN_CD_CATEGORIA NOT LIKE '1.102.009%'
  		AND PM.CAPN_CD_CATEGORIA NOT LIKE '2.504.001%'
  
  ) T1

GROUP BY
	T1.Filial
	,T1.Produto", pattern = " ")

# Base reversa (consReversa) ====
consultaReversa <- str_c("
SELECT 
	CAST(PF.FILI_CD_FILIAL AS NUMERIC) AS FILIAL,
	CAST(PF.PRME_CD_PRODUTO AS NUMERIC) AS PRODUTO,
	CAST(F.CODIGO_DEPOSITO_PRINCIPAL AS NUMERIC) AS DEPOSITO,
	CAST(PM.PRME_NR_DV AS NUMERIC) AS NR_DV,
	PF.PRFI_FL_SITUACAO AS SITUACAO,
	CAST(ISNULL(PF.ESTQUE_ALVO, 0) AS NUMERIC) AS EA,
	IIF((PF.PRFI_DT_FIMVALFARE > GETDATE() AND PF.PRFI_QT_UNDFAC > PF.ESTQUE_ALVO), 'A', 'D') AS SIT_FAC,
	CAST(IIF((PF.PRFI_DT_FIMVALFARE > GETDATE() AND  PF.PRFI_QT_UNDFAC > PF.ESTQUE_ALVO), 
		PF.PRFI_QT_UNDFAC, 0) AS NUMERIC) AS QTD_FAC,
	CAST(PF.PRFI_QT_ESTOQATUAL AS NUMERIC) AS QT_ESTOQATUAL,
	CAST(PF.PRFI_QT_ESTOQATUAL * PF.PRFI_VL_CMPG AS MONEY) AS VL_ESTOQUE,
	CAST(PF.PRFI_VL_CMPG AS MONEY) AS VL_CMPG,
	CAST(PF.PRFI_DT_ULTVENDA AS DATE) AS DT_ULTVENDA

FROM DBO.PRODUTO_FILIAL PF 
	INNER JOIN DBO.FILIAL F ON PF.FILI_CD_FILIAL = F.FILI_CD_FILIAL 
	INNER JOIN DBO.PRODUTO_MESTRE PM ON PF.PRME_CD_PRODUTO = PM.PRME_CD_PRODUTO 

WHERE 
	CONVERT(DATE, PM.XXXX_DH_CAD) < DATEADD(DAY, 1, DATEADD(MONTH,-5, EOMONTH(GETDATE())))
	AND CONVERT(DATE, PF.XXXX_DH_CAD) < DATEADD(DAY, 1, DATEADD(MONTH,-5, EOMONTH(GETDATE())))
	AND CAST(PF.PRFI_QT_ESTOQATUAL AS NUMERIC) > 0
	--AND (DATEDIFF(MONTH, PF.PRFI_DT_ULTVENDA, GETDATE()) >= 3 OR PF.PRFI_DT_ULTVENDA IS NULL)", pattern = " ")

# Base venda media dia cd (cd_vmd) ====

cd_vmd <- str_c("DECLARE @DATA01 DATE
  DECLARE @DATA02 DATE
  DECLARE @DATA03 DATE
  DECLARE @DATA04 DATE
  DECLARE @DATA05 DATE
  DECLARE @DATA06 DATE
  DECLARE @DATA07 DATE
  SET @DATA01= DATEADD(DAY,-181,GETDATE())
  SET @DATA02= DATEADD(DAY,30,@DATA01)
  SET @DATA03= DATEADD(DAY,30,@DATA02)
  SET @DATA04= DATEADD(DAY,30,@DATA03)
  SET @DATA05= DATEADD(DAY,30,@DATA04)
  SET @DATA06= DATEADD(DAY,30,@DATA05)
  SET @DATA07= DATEADD(DAY,30,@DATA06)
  
  SELECT
  	T1.CD
  	,T1.Produto
    ,CAST( SUM(
		CASE 
            WHEN (T1.DT_OCORR_REAL > @DATA01 AND T1.DT_OCORR_REAL <= @DATA02) 
            AND T1.QTDVENDA > 0 THEN T1.QTDVENDA  *0.03
            WHEN (T1.DT_OCORR_REAL > @DATA02 AND T1.DT_OCORR_REAL <= @DATA03) 
            AND T1.QTDVENDA > 0 THEN T1.QTDVENDA * 0.07
            WHEN (T1.DT_OCORR_REAL > @DATA03  AND T1.DT_OCORR_REAL <= @DATA04) 
            AND T1.QTDVENDA>0 THEN T1.QTDVENDA * 0.1
            WHEN (T1.DT_OCORR_REAL > @DATA04 AND T1.DT_OCORR_REAL <= @DATA05) 
            AND T1.QTDVENDA>0 THEN T1.QTDVENDA * 0.2
            WHEN (T1.DT_OCORR_REAL>@DATA05 AND T1.DT_OCORR_REAL<=@DATA06) 
            AND T1.QTDVENDA>0 THEN T1.QTDVENDA * 0.27
            WHEN (T1.DT_OCORR_REAL>@DATA06 AND T1.DT_OCORR_REAL<=@DATA07) 
            AND T1.QTDVENDA>0 THEN T1.QTDVENDA * 0.33
            ELSE 0 
		END)/30 AS DECIMAL(20,6)) AS VMD

  FROM (
  	SELECT
  	      FL.CODIGO_DEPOSITO_PRINCIPAL AS CD
          ,KF.KAFI_CD_PRODUTO AS Produto
          ,KF.KAFI_QT_MOV AS QTDVENDA
          ,CONVERT(DATE, KF.KAFI_DH_OCORRREAL) AS DT_OCORR_REAL
      
      FROM KARDEX_FILIAL KF WITH(NOLOCK)
              
      INNER JOIN PRODUTO_MESTRE PM WITH(NOLOCK)
        ON KF.KAFI_CD_PRODUTO = PM.PRME_CD_PRODUTO
      INNER JOIN FILIAL FL WITH(NOLOCK)
        ON KF.KAFI_CD_FILIAL = FL.FILI_CD_FILIAL
              
      WHERE 
          KF.KAFI_TP_MOV IN('SV')
          AND CONVERT(DATE, KF.KAFI_DH_OCORRREAL) BETWEEN 
          @DATA01 AND @DATA07
          AND CONVERT(DATE,PM.XXXX_DH_CAD) < @DATA01 AND CONVERT(DATE,FL.XXXX_DH_CAD) < @DATA01
          AND PM.CAPN_CD_CATEGORIA NOT LIKE '3%'
          AND PM.CAPN_CD_CATEGORIA NOT LIKE '1.101.009%'
          AND PM.CAPN_CD_CATEGORIA NOT LIKE '1.102.009%'
          AND PM.CAPN_CD_CATEGORIA NOT LIKE '2.504.001%'
  
  	UNION ALL
  
  	SELECT
  		FL.CODIGO_DEPOSITO_PRINCIPAL AS CD
  		,KFD.KAFI_CD_PRODUTO AS CODPROD
  		,KFD.KAFI_QT_MOV AS QTDVENDA
  		,KFD.KAFI_DH_OCORRREAL AS DT_OCORR_REAL
  	
  	FROM KARDEX_FILIAL_DIA KFD WITH(NOLOCK)
          
  	INNER JOIN PRODUTO_MESTRE PM WITH(NOLOCK)
  	  ON KFD.KAFI_CD_PRODUTO = PM.PRME_CD_PRODUTO
  	INNER JOIN FILIAL FL WITH(NOLOCK)
  	  ON KFD.KAFI_CD_FILIAL = FL.FILI_CD_FILIAL
          
  	WHERE 
  		KFD.KAFI_TP_MOV IN('SV')
  		AND CONVERT(DATE,KFD.KAFI_DH_OCORRREAL) BETWEEN @DATA01 AND @DATA07
  		AND CONVERT(DATE,PM.XXXX_DH_CAD) <@DATA01 AND FL.XXXX_DH_CAD <@DATA01
  		AND PM.CAPN_CD_CATEGORIA NOT LIKE '3%'
  		AND PM.CAPN_CD_CATEGORIA NOT LIKE '1.101.009%'
  		AND PM.CAPN_CD_CATEGORIA NOT LIKE '1.102.009%'
  		AND PM.CAPN_CD_CATEGORIA NOT LIKE '2.504.001%'
  ) T1

GROUP BY
	T1.CD
	,T1.Produto", pattern = " ")

# Base base cd (cd) ====
cd <- str_c("SELECT 
PD.DEPO_CD_DEPOSITO AS CD_DEP,
PD.PRME_CD_PRODUTO AS CD_PRODUTO,
CONVERT(DATE, PD.PRDP_DT_ULTCOMPRA) AS DT_ULTCOMPRA,
CAST(PD.PRDP_QT_ESTOQATUAL AS NUMERIC) AS QT_ESTQATUAL,
CAST(PD.PRDP_VL_CMPG AS MONEY) AS VL_CMPG,
CAST(PD.PRDP_QT_ESTOQATUAL * PD.PRDP_VL_CMPG AS MONEY) AS VL_ESTATUAL,
PD.PRDP_TP_CLABCFAT AS TP_ABC,
pd.PRDP_FL_SITUACAO as FL_SITUACAO

FROM PRODUTO_DEPOSITO PD

INNER JOIN PRODUTO_MESTRE PM WITH(NOLOCK)
ON PD.PRME_CD_PRODUTO = PM.PRME_CD_PRODUTO

WHERE
PM.CAPN_CD_CATEGORIA NOT LIKE '3%'
AND PM.CAPN_CD_CATEGORIA NOT LIKE '1.101.009%'
AND PM.CAPN_CD_CATEGORIA NOT LIKE '1.102.009%'
AND PM.CAPN_CD_CATEGORIA NOT LIKE '2.504.001%'", pattern = " ")


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
