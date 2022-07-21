# ETL 05.01 - Clusterização 01
    # Objetivo: Popular as tabelas no banco DDTM_OiMasterDados
    # Desenvolvedor: Marcelo Muniz de Alencar
################################################################

rm (list = ls ()) #exclui todos os objetos na memoria

#SetaAmbienteDeTrabalho#########################################

# Definindo area de trabalho 
setwd("C:/Users/MarceloMuniz/OneDrive/# Oi Master Dados - 3ª Fase/_TrabalhoFinal/docsDrive")
#getwd()

#CarregaPacotes#################################################

# Importando pacotes
#install.packages("readr")
library(readr)
#install.packages("caret")
library(caret)
#install.packages("klaR")
library(klaR)
#install.packages("cluster")
library(cluster)
#install.packages("RODBC")
library(RODBC)
#install.packages("stats")
library(stats)
#install.packages("dplyr")
library(dplyr)
#install.packages("ggplot2")
library(ggplot2)
#install.packages("ggfortify")
library(ggfortify)
#install.packages("NbClust")
library(NbClust)
#install.packages("factoextra")
library(factoextra)

##################### create dataframe #######################

# criando conexao
dbcon = odbcDriverConnect('driver={SQL Server};
                                  server=PCMARCELO\\SQLEXPRESS;
                                 database=DDTM_OiMasterDados;
                                 trusted_connection=true')

# criando conexao
dbcon2 = odbcDriverConnect('driver={SQL Server};
                                  server=PCMARCELO\\SQLEXPRESS;
                                 database=DSTG_OiMasterDados;
                                 trusted_connection=true')

# criando dataframe
df <- sqlQuery(dbcon,
                    " 
declare @MediaTemperatura as float
declare @DesvioPadraoTemperatura as float

set @MediaTemperatura = 
(select  AVG(ROUND(([ValMedioTemperaturaC]),2))
from [OiMasterDados].[TOiFAT_Temperatura] (NOLOCK))

set @DesvioPadraoTemperatura = 
(select  STDEVP(ROUND(([ValMedioTemperaturaC]),2))
from [OiMasterDados].[TOiFAT_Temperatura] (NOLOCK))

select	tt.CodSkCidade,
		tt.NomCidade,
		tt.ValLatitude,
		tt.ValLongitude,
		tt.NumAno,
		tt.DescEstacao,
		MIN(tt.CodBkData) as DatIniEstacao,
		MIN(tt.CodBkData) as DatMaxEstacao,
		AVG(tt.ValMedioTemperaturaC) as ValMedioTemperatura
from
(select t1.CodSkData,
		t3.CodBkData,
	   t3.NumAno,
	   case when t2.FlgHemisferioNorte = 'S'
				then t3.DescEstacaoBoreal
				else t3.DescEstacaoAustral
	   end as DescEstacao,
	   t1.CodSkCidade,
	   t2.NomCidade,
	   t2.NomEstado,
	   t2.NomPais,
	   t2.NomRegiaoGlobo,
	   t2.ValLatitude,
	   t2.ValLongitude,
	   t1.ValMedioTemperaturaC,
	   (t1.ValMedioTemperaturaC-@MediaTemperatura)/@DesvioPadraoTemperatura as EscZGlobo
from [OiMasterDados].[TOiFAT_Temperatura] as t1 (NOLOCK)
		inner join
	 [OiMasterDados].[TOiDIM_Cidade] as t2 (NOLOCK)
		on t1.CodSkCidade = t2.CodSkCidade
		inner join
	 [OiMasterDados].[TOiDIM_Calendario] as t3 (NOLOCK)
		on t1.CodSkData = t3.CodSkData
--- retira os outliers 
where (t1.ValMedioTemperaturaC-@MediaTemperatura)/@DesvioPadraoTemperatura > -3
and (t1.ValMedioTemperaturaC-@MediaTemperatura)/@DesvioPadraoTemperatura < 3) as tt
group by tt.CodSkCidade,
		 tt.NomCidade,
		 tt.ValLatitude,
		 tt.ValLongitude,
		 tt.NumAno,
		 tt.DescEstacao
order by 1,5,7")

#View(df)

####################### escoreZ valores ##############################
df1 = cbind(as.double(df$ValLatitude),
            as.double(df$ValLongitude),
            as.double(df$ValMedioTemperatura))
df1 = as.data.frame(df1)
names(df1)[]<-c("ValLatitude_EscZ","ValLongitude_Escz","ValMedioTemperaturaC_EscZ")
#View(head(df1))

#find z-scores of each column
df2 = sapply(df1, function(df1) (df1-mean(df1))/sd(df1))
#View(head(df2))

df_final1 = cbind(df, df2)
#View(df_final1)

####################### Min Max ##############################
df1_1 = cbind(as.double(df$ValLatitude),
              as.double(df$ValLongitude),
              as.double(df$ValMedioTemperatura))
df1_1 = as.data.frame(df1_1)
names(df1_1)[]<-c("ValLatitude_MinMax","ValLongitude_MinMax","ValMedioTemperaturaC_MinMax")
#View(head(df1_1))

#find min-max of each column
df2_1 = sapply(df1_1, function(df1_1) (df1_1-min(df1_1))/(max(df1_1)-min(df1_1)))
#View(head(df2_1))

df_final1_1 = cbind(df_final1, df2_1)
#View(df_final1_1)

####################### dummies ##############################
df3 = cbind(df$DescEstacao)
df3 = as.data.frame(df3)
names(df3)[]<-c("Estacao")
#View(head(df3))

dummy <- dummyVars(~ ., data = df3)
df4 <- predict(dummy, df3)
#View(head(df4))

df_final2 = cbind(df_final1_1, df4)
#View(df_final2)

####################### gravar tabela ##########################
# cria lista de tabela da conexao dbcon2
listaTabelas <- sqlTables(dbcon2, as.is = TRUE)
#View(head(listaTabelas))

if (sum(listaTabelas$TABLE_NAME == 'TOiSTG_BaseClusterR') == 1) {
  sqlDrop(dbcon2, "OiMasterDados.TOiSTG_BaseClusterR")
} 
sqlSave(dbcon2, dat = df_final2, "OiMasterDados.TOiSTG_BaseClusterR", rownames = FALSE)

####################### K-MEANS ##############################
# criando dataframe 
df5 <- sqlQuery(dbcon2,
               "SELECT 
                      [ValMedioTemperaturaC_EscZ]
                      ,[EstacaoInverno]
                      ,[EstacaoOutono]
                      ,[EstacaoPrimavera]
                FROM [DSTG_OiMasterDados].[OiMasterDados].[TOiSTG_BaseClusterR] (NOLOCK)
                ORDER BY CodSkCidade, NumAno")
View(df5)

# Graficos para definir quantidade de clusters 
#fviz_nbclust(df5, kmeans, method = "wss", k.max = 10)
#fviz_nbclust(df5, kmeans, method = "silhouette", k.max = 10)
 
# Gerar CLuster 
modelo <- kmeans(df5, centers = 6)
av_Modelo <- modelo$betweenss / modelo$totss
df_modelo <- as.data.frame(modelo$cluster)
colnames(df_modelo)[1] <- 'Cluster'
print(av_Modelo)
#View(df_modelo)
 
# Graficos com a divisao dos clusters 
#autoplot(modelo,df5,frame=TRUE)

# gerar dataframe base para carga no banco
df_final4 <- cbind(df_final2$CodSkCidade,
                   df_final2$NumAno,
                   df_final2$DescEstacao,
                   as.double(df_final2$ValMedioTemperatura),
                   df_modelo$Cluster)
df_final4 = as.data.frame(df_final4)
names(df_final4)[]<-c("CodSkCidade","NumAno","DescEstacao","ValMedioTemperatura","Cluster")
#View(df_final4)

####################### gravar tabela K-MEANS ##########################
# cria lista de tabela da conexao dbcon
listaTabelas2 <- sqlTables(dbcon2, as.is = TRUE)
#View(listaTabelas2)

if (sum(listaTabelas2$TABLE_NAME == 'TOiSTG_BaseClusterRFinal') == 1) {
  sqlDrop(dbcon2, "OiMasterDados.TOiSTG_BaseClusterRFinal")
} 
sqlSave(dbcon2, dat = df_final4, "OiMasterDados.TOiSTG_BaseClusterRFinal", rownames = FALSE)

####################### gravar tabela com Categoria de Estação por cidade ##########################

sqlQuery(dbcon2,
                "SET NOCOUNT ON

DELETE FROM [OiMasterDados].[TOiSTG_BaseClusterCategoriaEstacao];

DECLARE 
    @cidade VARCHAR(200);

DECLARE cursor_cidade CURSOR
FOR select NumCidade
	from [OiMasterDados].[TOiSTG_Cidade] (nolock)
	order by NumCidade

OPEN cursor_cidade

FETCH NEXT FROM cursor_cidade INTO 
    @cidade

WHILE @@FETCH_STATUS = 0
    BEGIN

INSERT INTO [OiMasterDados].[TOiSTG_BaseClusterCategoriaEstacao]
        select	ta.CodSkCidade,
		ta.NomCidade,
		ta.MinimaCidade,
		ta.MediaCidade,
		ta.MaximaCidade,
		ta.DesvPadCidade,
		tb.Categoria + ' (de ' +  convert(varchar(10),tc.QtdCluster) + ')' as Categoria
from
(select ttt.*
from
(select tt.*, RANK() OVER   
			(PARTITION BY tt.[DescEstacao] ORDER BY tt.Contagem DESC) AS RKG
from
(select t2.NomCidade,
		t2.CodSkCidade,
		[Cluster],
		[DescEstacao],
		min(CONVERT(FLOAT,[ValMedioTemperatura])) as MinimaCidade,
		avg(CONVERT(FLOAT,[ValMedioTemperatura])) as MediaCidade,
		max(CONVERT(FLOAT,[ValMedioTemperatura])) as MaximaCidade,
		STDEVP(CONVERT(FLOAT,[ValMedioTemperatura])) as DesvPadCidade,
		COUNT(*) as Contagem,
		COUNT(distinct t1.CodSkCidade) as QtdCidades
  FROM [DSTG_OiMasterDados].[OiMasterDados].[TOiSTG_BaseClusterRFinal] as t1 (NOLOCK)
			inner join
		 [DDTM_OiMasterDados].[OiMasterDados].[TOiDIM_Cidade] as t2 (nolock)
			on t1.CodSkCidade = t2.CodSkCidade
	where t2.CodSkCidade = @cidade
  group by [Cluster],[DescEstacao],t2.NomCidade,t2.CodSkCidade) as tt) as ttt
  where ttt.RKG = 1) as ta
inner join
(select [Cluster], t1.[DescEstacao],
		min(CONVERT(FLOAT,[ValMedioTemperatura])) as Minima,
		avg(CONVERT(FLOAT,[ValMedioTemperatura])) as Media,
		max(CONVERT(FLOAT,[ValMedioTemperatura])) as Maxima,
		STDEVP(CONVERT(FLOAT,[ValMedioTemperatura])) as DesvPAd,
		COUNT(*) as Contagem,
		COUNT(distinct CodSkCidade) as QtdCidades,
		t1.[DescEstacao] + ' ' + convert(varchar(10),RANK() OVER   
			(PARTITION BY t1.[DescEstacao] ORDER BY avg(CONVERT(FLOAT,[ValMedioTemperatura]))))  AS Categoria
  FROM [DSTG_OiMasterDados].[OiMasterDados].[TOiSTG_BaseClusterRFinal] as t1 (NOLOCK)
  group by [Cluster], t1.[DescEstacao]
  )as tb
	on ta.DescEstacao = tb.DescEstacao
	and ta.Cluster = tb.Cluster
	inner join
 (
 select [DescEstacao],
		  COUNT(distinct Cluster) as QtdCluster
  FROM [DSTG_OiMasterDados].[OiMasterDados].[TOiSTG_BaseClusterRFinal] (NOLOCK)
  group by [DescEstacao]
 ) as tc
 on ta.DescEstacao = tc.DescEstacao
  order by Categoria

        FETCH NEXT FROM cursor_cidade INTO @cidade
    END

DEALLOCATE cursor_cidade;")

####################### desconecta BD ##########################
close(dbcon)
close(dbcon2)