# Databricks notebook source
# ==========================================================
# PROJETO 2 - ANÁLISE DE VENDAS OLIST
# ETAPA 4 - CARREGAMENTO DOS PEDIDOS
# ==========================================================

from pyspark.sql import functions as F

# Caminho do volume
caminho_base = "/Volumes/workspace/default/projeto_olist"

# Carregamento da tabela de pedidos
df_pedidos = (
    spark.read
    .format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load(f"{caminho_base}/olist_orders_dataset.csv")
)

print("=== TABELA DE PEDIDOS CARREGADA ===")

print(f"Quantidade de registros: {df_pedidos.count()}")

print("\n=== ESTRUTURA ===")
df_pedidos.printSchema()

print("\n=== PRIMEIROS REGISTROS ===")
display(df_pedidos.limit(10))

# COMMAND ----------

# ETAPA 5 - CARREGAMENTO DOS ITENS DOS PEDIDOS

df_itens = (
    spark.read
    .format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load(f"{caminho_base}/olist_order_items_dataset.csv")
)

print("=== QUANTIDADE DE ITENS ===")
print(df_itens.count())

print("\n=== ESTRUTURA ===")
df_itens.printSchema()

display(df_itens.limit(10))

# COMMAND ----------

# ETAPA 5.1 - PEDIDOS DISTINTOS E ITENS

from pyspark.sql.functions import countDistinct

total_itens = df_itens.count()

pedidos_distintos = (
    df_itens
    .select(countDistinct("order_id").alias("total"))
    .first()["total"]
)

print(f"Total de itens: {total_itens}")
print(f"Pedidos distintos com itens: {pedidos_distintos}")
print(f"Diferença: {total_itens - pedidos_distintos}")

# COMMAND ----------

# ETAPA 5.2 - RELACIONAMENTO ENTRE PEDIDOS E ITENS

df_pedidos_itens = (
    df_pedidos.alias("p")
    .join(
        df_itens.alias("i"),
        on="order_id",
        how="left"
    )
)

print("=== RESULTADO DO JOIN ===")

print(f"Linhas após JOIN: {df_pedidos_itens.count()}")

print(
    "Pedidos distintos após JOIN:",
    df_pedidos_itens.select("order_id").distinct().count()
)

display(
    df_pedidos_itens.select(
        "order_id",
        "order_status",
        "order_item_id",
        "product_id",
        "price",
        "freight_value"
    ).limit(10)
)

# COMMAND ----------

# ETAPA 5.3 - PEDIDOS SEM ITENS

df_sem_itens = (
    df_pedidos_itens
    .filter(F.col("order_item_id").isNull())
)

print("=== PEDIDOS SEM ITENS ===")

print(f"Quantidade: {df_sem_itens.count()}")

print("\n=== DISTRIBUIÇÃO POR STATUS ===")

display(
    df_sem_itens
    .groupBy("order_status")
    .count()
    .orderBy(F.desc("count"))
)

# COMMAND ----------

# ETAPA 5.4 - DISTRIBUIÇÃO GERAL DOS PEDIDOS

df_status_pedidos = (
    df_pedidos
    .groupBy("order_status")
    .count()
    .orderBy(F.desc("count"))
)

print("=== STATUS DE TODOS OS PEDIDOS ===")

display(df_status_pedidos)

# COMMAND ----------

# ETAPA 5.5 - VALOR DOS PRODUTOS ENTREGUES

df_vendas_entregues = (
    df_pedidos_itens
    .filter(F.col("order_status") == "delivered")
    .filter(F.col("price").isNotNull())
)

valor_produtos = (
    df_vendas_entregues
    .agg(
        F.round(F.sum("price"), 2).alias("valor_total")
    )
    .first()["valor_total"]
)

pedidos_entregues = (
    df_vendas_entregues
    .select("order_id")
    .distinct()
    .count()
)

print("=== INDICADORES COMERCIAIS ===")
print(f"Pedidos entregues com itens: {pedidos_entregues}")
print(f"Valor dos produtos entregues: R$ {valor_produtos:,.2f}")

# COMMAND ----------

# ETAPA 5.6 - TICKET MÉDIO

ticket_medio = (
    valor_produtos / pedidos_entregues
    if pedidos_entregues > 0
    else 0
)

print("=== TICKET MÉDIO ===")
print(f"Valor dos produtos: R$ {valor_produtos:,.2f}")
print(f"Pedidos entregues: {pedidos_entregues}")
print(f"Ticket médio: R$ {ticket_medio:,.2f}")

# COMMAND ----------

# ETAPA 5.7 - ANÁLISE DO FRETE

df_indicadores_frete = (
    df_vendas_entregues
    .agg(
        F.round(F.sum("price"), 2).alias("valor_produtos"),
        F.round(F.sum("freight_value"), 2).alias("valor_frete")
    )
    .withColumn(
        "valor_total_com_frete",
        F.round(
            F.col("valor_produtos") + F.col("valor_frete"),
            2
        )
    )
    .withColumn(
        "percentual_frete",
        F.round(
            (F.col("valor_frete") / F.col("valor_produtos")) * 100,
            2
        )
    )
)

display(df_indicadores_frete)

# COMMAND ----------

# ETAPA 6.1 - VENDAS POR ANO

df_vendas_ano = (
    df_vendas_entregues
    .withColumn(
        "ano",
        F.year("order_purchase_timestamp")
    )
    .groupBy("ano")
    .agg(
        F.round(
            F.sum("price"), 2
        ).alias("valor_produtos"),

        F.countDistinct(
            "order_id"
        ).alias("total_pedidos")
    )
    .orderBy("ano")
)

print("=== VENDAS POR ANO ===")

display(df_vendas_ano)

# COMMAND ----------

# ETAPA 6.2 - VENDAS MENSAIS

df_vendas_mensais = (
    df_vendas_entregues
    .withColumn(
        "ano_mes",
        F.date_format("order_purchase_timestamp", "yyyy-MM")
    )
    .groupBy("ano_mes")
    .agg(
        F.round(F.sum("price"), 2).alias("valor_produtos"),
        F.countDistinct("order_id").alias("total_pedidos")
    )
    .orderBy("ano_mes")
)

print("=== EVOLUÇÃO MENSAL DAS VENDAS ===")

display(df_vendas_mensais)

# COMMAND ----------

# ETAPA 6.3 - CRESCIMENTO MENSAL

from pyspark.sql.window import Window

# Janela para ordenar os meses
janela_mensal = Window.orderBy("ano_mes")

df_crescimento_mensal = (
    df_vendas_mensais
    .withColumn(
        "valor_mes_anterior",
        F.lag("valor_produtos").over(janela_mensal)
    )
    .withColumn(
        "crescimento_percentual",
        F.when(
            F.col("valor_mes_anterior") > 0,
            F.round(
                (
                    (F.col("valor_produtos") -
                     F.col("valor_mes_anterior"))
                    / F.col("valor_mes_anterior")
                ) * 100,
                2
            )
        )
    )
)

display(df_crescimento_mensal)

# COMMAND ----------

# ETAPA 6.4 - ANÁLISE DO PICO DE NOVEMBRO

df_comparativo_2017 = (
    df_vendas_mensais
    .filter(
        F.col("ano_mes").isin(
            "2017-10",
            "2017-11",
            "2017-12"
        )
    )
    .withColumn(
        "ticket_medio",
        F.round(
            F.col("valor_produtos") /
            F.col("total_pedidos"),
            2
        )
    )
    .orderBy("ano_mes")
)

print("=== COMPARATIVO OUT/NOV/DEZ 2017 ===")

display(df_comparativo_2017)

# COMMAND ----------

# ETAPA 6.5 - VALIDAR CRESCIMENTO DE NOVEMBRO

df_validacao_crescimento = (
    df_crescimento_mensal
    .filter(
        F.col("ano_mes").isin("2017-10", "2017-11", "2017-12")
    )
    .select(
        "ano_mes",
        "valor_produtos",
        "valor_mes_anterior",
        "crescimento_percentual"
    )
    .orderBy("ano_mes")
)

display(df_validacao_crescimento)

# COMMAND ----------

# ETAPA 7.1 - CARREGAMENTO DA TABELA DE PRODUTOS

df_produtos = (
    spark.read
    .format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load(f"{caminho_base}/olist_products_dataset.csv")
)

print("=== TABELA DE PRODUTOS ===")

print(f"Total de registros: {df_produtos.count()}")

print("\n=== ESTRUTURA ===")
df_produtos.printSchema()

print("\n=== PRIMEIROS REGISTROS ===")
display(df_produtos.limit(10))

# COMMAND ----------

# ETAPA 7.2 - VALIDAR CHAVE DOS PRODUTOS

total_produtos = df_produtos.count()

produtos_unicos = (
    df_produtos
    .select("product_id")
    .distinct()
    .count()
)

duplicados = total_produtos - produtos_unicos

print(f"Total de registros: {total_produtos}")
print(f"Produtos distintos: {produtos_unicos}")
print(f"Chaves duplicadas: {duplicados}")

# COMMAND ----------

# ETAPA 7.3 - JOIN ENTRE VENDAS E PRODUTOS

df_vendas_produtos = (
    df_vendas_entregues.alias("v")
    .join(
        df_produtos.select(
            "product_id",
            "product_category_name"
        ).alias("p"),
        on="product_id",
        how="left"
    )
)

print("=== VALIDAÇÃO DO JOIN ===")

print(
    f"Linhas antes do JOIN: {df_vendas_entregues.count()}"
)

print(
    f"Linhas depois do JOIN: {df_vendas_produtos.count()}"
)

print("=== PRIMEIROS REGISTROS ===")

display(
    df_vendas_produtos.select(
        "order_id",
        "product_id",
        "product_category_name",
        "price"
    ).limit(10)
)

# COMMAND ----------

# ETAPA 7.4 - RANKING DE CATEGORIAS

df_ranking_categorias = (
    df_vendas_produtos
    .groupBy("product_category_name")
    .agg(
        F.round(
            F.sum("price"), 2
        ).alias("valor_produtos"),

        F.countDistinct(
            "order_id"
        ).alias("total_pedidos")
    )
    .orderBy(
        F.desc("valor_produtos")
    )
)

print("=== TOP 10 CATEGORIAS ===")

display(df_ranking_categorias.limit(10))

# COMMAND ----------

# ETAPA 7.5 - PARTICIPAÇÃO PERCENTUAL DAS CATEGORIAS

from pyspark.sql.window import Window

# Janela que considera todas as categorias
janela_total = Window.partitionBy(F.lit(1))

df_participacao_categorias = (
    df_ranking_categorias
    .withColumn(
        "total_geral",
        F.sum("valor_produtos").over(janela_total)
    )
    .withColumn(
        "participacao_percentual",
        F.round(
            F.col("valor_produtos") /
            F.col("total_geral") * 100,
            2
        )
    )
    .orderBy(F.desc("valor_produtos"))
)

print("=== PARTICIPAÇÃO DAS CATEGORIAS ===")

df_participacao_categorias.show(10, truncate=False)

# COMMAND ----------

# ETAPA 7.6 - CONCENTRAÇÃO DO TOP 5 CATEGORIAS

from pyspark.sql import functions as F

# Selecionar as cinco maiores categorias
df_top5 = (
    df_participacao_categorias
    .orderBy(F.desc("valor_produtos"))
    .limit(5)
)

# Calcular os indicadores
resultado_top5 = (
    df_top5
    .agg(
        F.round(
            F.sum("valor_produtos"), 2
        ).alias("valor_top5"),

        F.round(
            F.sum("participacao_percentual"), 2
        ).alias("participacao_top5")
    )
)

print("=== TOP 5 CATEGORIAS ===")
df_top5.select(
    "product_category_name",
    "valor_produtos",
    "participacao_percentual"
).show(truncate=False)

print("=== CONCENTRAÇÃO DO TOP 5 ===")
display(resultado_top5)

# COMMAND ----------

# ETAPA 8.1 - CARREGAMENTO DA TABELA DE CLIENTES

df_clientes = (
    spark.read
    .format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load(f"{caminho_base}/olist_customers_dataset.csv")
)

print("=== TABELA DE CLIENTES ===")

print(f"Total de registros: {df_clientes.count()}")

print("\n=== ESTRUTURA ===")
df_clientes.printSchema()

print("\n=== PRIMEIROS REGISTROS ===")
display(df_clientes.limit(10))

# COMMAND ----------

# ETAPA 8.2 - VALIDAÇÃO DA CHAVE DOS CLIENTES

total_clientes = df_clientes.count()

clientes_unicos = (
    df_clientes
    .select("customer_id")
    .distinct()
    .count()
)

chaves_duplicadas = total_clientes - clientes_unicos

print("=== VALIDAÇÃO DOS CLIENTES ===")

print(f"Total de registros: {total_clientes}")
print(f"Clientes distintos por customer_id: {clientes_unicos}")
print(f"Chaves duplicadas: {chaves_duplicadas}")

# COMMAND ----------

# ==========================================================
# ETAPA 8.3 - JOIN ENTRE VENDAS E CLIENTES
# ==========================================================

df_vendas_clientes = (
    df_vendas_produtos.alias("v")
    .join(
        df_clientes.select(
            "customer_id",
            "customer_city",
            "customer_state"
        ).alias("c"),
        on="customer_id",
        how="left"
    )
)

print("=== VALIDAÇÃO DO JOIN ===")

print(
    f"Linhas antes do JOIN: {df_vendas_produtos.count()}"
)

print(
    f"Linhas depois do JOIN: {df_vendas_clientes.count()}"
)

print("=== PRIMEIROS REGISTROS ===")

display(
    df_vendas_clientes.select(
        "order_id",
        "customer_city",
        "customer_state",
        "product_category_name",
        "price"
    ).limit(10)
)


# COMMAND ----------

# ==========================================================
# ETAPA 8.4 - RANKING DE VENDAS POR ESTADO
# ==========================================================

df_vendas_estado = (
    df_vendas_clientes
    .groupBy("customer_state")
    .agg(
        F.round(
            F.sum("price"), 2
        ).alias("valor_produtos"),

        F.countDistinct(
            "order_id"
        ).alias("total_pedidos")
    )
    .withColumn(
        "ticket_medio",
        F.round(
            F.col("valor_produtos") /
            F.col("total_pedidos"),
            2
        )
    )
    .orderBy(F.desc("valor_produtos"))
)

print("=== TOP 10 ESTADOS POR VALOR DOS PRODUTOS ===")

df_vendas_estado.show(10, truncate=False)

# COMMAND ----------

# ==========================================================
# ETAPA 8.5 - PARTICIPAÇÃO PERCENTUAL POR ESTADO
# ==========================================================

df_participacao_estados = (
    df_vendas_estado
    .withColumn(
        "participacao_percentual",
        F.round(
            F.col("valor_produtos") /
            F.sum("valor_produtos").over(
                Window.partitionBy(F.lit(1))
            ) * 100,
            2
        )
    )
    .orderBy(F.desc("valor_produtos"))
)

print("=== TOP 10 ESTADOS ===")

df_participacao_estados.show(10, truncate=False)

# COMMAND ----------

# ==========================================================
# ETAPA 8.6 - CONCENTRAÇÃO DOS 3 MAIORES ESTADOS
# ==========================================================

df_top3_estados = (
    df_participacao_estados
    .orderBy(F.desc("valor_produtos"))
    .limit(3)
)

print("=== TOP 3 ESTADOS ===")

df_top3_estados.select(
    "customer_state",
    "valor_produtos",
    "total_pedidos",
    "participacao_percentual"
).show(truncate=False)

# Participação calculada diretamente sobre os valores
valor_top3 = df_top3_estados.agg(
    F.sum("valor_produtos")
).first()[0]

valor_geral = df_participacao_estados.agg(
    F.sum("valor_produtos")
).first()[0]

participacao_top3 = (
    valor_top3 / valor_geral * 100
    if valor_geral else 0
)

print("\n=== CONCENTRAÇÃO GEOGRÁFICA ===")
print(f"Valor dos 3 maiores estados: R$ {valor_top3:,.2f}")
print(f"Participação dos 3 maiores estados: {participacao_top3:.2f}%")

# COMMAND ----------

# ==========================================================
# ETAPA 8.7 - RANKING DE TICKET MÉDIO POR ESTADO
# ==========================================================

df_ranking_ticket_estado = (
    df_vendas_estado
    .orderBy(F.desc("ticket_medio"))
)

print("=== TOP 10 ESTADOS POR TICKET MÉDIO ===")

df_ranking_ticket_estado.select(
    "customer_state",
    "total_pedidos",
    "valor_produtos",
    "ticket_medio"
).show(10, truncate=False)

# COMMAND ----------

# ==========================================================
# ETAPA 8.8 - COMPARAÇÃO DO TICKET MÉDIO
# ==========================================================

ticket_pb = (
    df_vendas_estado
    .filter(F.col("customer_state") == "PB")
    .select("ticket_medio")
    .first()[0]
)

ticket_geral = (
    df_vendas_entregues
    .agg(
        (
            F.sum("price") /
            F.countDistinct("order_id")
        ).alias("ticket_medio_geral")
    )
    .first()[0]
)

diferenca_percentual = (
    (ticket_pb / ticket_geral) - 1
) * 100

print("=== COMPARAÇÃO DE TICKET MÉDIO ===")

print(f"Ticket médio PB: R$ {ticket_pb:.2f}")
print(f"Ticket médio geral: R$ {ticket_geral:.2f}")
print(f"Diferença percentual: {diferenca_percentual:.2f}%")

# COMMAND ----------

# ==========================================================
# ETAPA 8.9 - CATEGORIAS MAIS VENDIDAS NA PARAÍBA
# ==========================================================

df_categorias_pb = (
    df_vendas_clientes
    .filter(F.col("customer_state") == "PB")
    .groupBy("product_category_name")
    .agg(
        F.round(
            F.sum("price"), 2
        ).alias("valor_produtos"),

        F.countDistinct(
            "order_id"
        ).alias("total_pedidos")
    )
    .orderBy(F.desc("valor_produtos"))
)

print("=== TOP 10 CATEGORIAS NA PARAÍBA ===")

df_categorias_pb.show(10, truncate=False)

# COMMAND ----------

# ==========================================================
# ETAPA 8.10 - VALOR TOTAL DE CADA PEDIDO NA PARAÍBA
# ==========================================================

df_pedidos_pb = (
    df_vendas_clientes
    .filter(F.col("customer_state") == "PB")
    .groupBy("order_id")
    .agg(
        F.round(
            F.sum("price"), 2
        ).alias("valor_pedido")
    )
)

print("=== ANÁLISE DOS PEDIDOS DA PARAÍBA ===")

df_pedidos_pb.agg(
    F.count("*").alias("total_pedidos"),
    F.round(F.avg("valor_pedido"), 2).alias("media"),
    F.round(F.median("valor_pedido"), 2).alias("mediana"),
    F.round(F.max("valor_pedido"), 2).alias("maior_pedido")
).show(truncate=False)

# COMMAND ----------

# ==========================================================
# ETAPA 8.11 - TOP 10 PEDIDOS DE MAIOR VALOR NA PARAÍBA
# ==========================================================

print("=== TOP 10 MAIORES PEDIDOS DA PARAÍBA ===")

df_pedidos_pb.orderBy(
    F.desc("valor_pedido")
).show(10, truncate=False)

# COMMAND ----------

# ==========================================================
# ETAPA 8.12 - CÁLCULO DO IQR NA PARAÍBA
# ==========================================================

quartis = df_pedidos_pb.approxQuantile(
    "valor_pedido",
    [0.25, 0.75],
    0.01
)

q1 = quartis[0]
q3 = quartis[1]

iqr = q3 - q1

limite_superior = q3 + (1.5 * iqr)

print("=== ANÁLISE DE OUTLIERS - PARAÍBA ===")

print(f"Q1: R$ {q1:.2f}")
print(f"Q3: R$ {q3:.2f}")
print(f"IQR: R$ {iqr:.2f}")
print(f"Limite superior: R$ {limite_superior:.2f}")

# COMMAND ----------

# ==========================================================
# ETAPA 8.13 - IMPACTO DOS POSSÍVEIS OUTLIERS NA PARAÍBA
# ==========================================================

df_outliers_pb = (
    df_pedidos_pb
    .filter(F.col("valor_pedido") > limite_superior)
)

total_pedidos_pb = df_pedidos_pb.count()

total_outliers = df_outliers_pb.count()

valor_total_pb = df_pedidos_pb.agg(
    F.sum("valor_pedido")
).first()[0]

valor_outliers = df_outliers_pb.agg(
    F.sum("valor_pedido")
).first()[0]

percentual_pedidos = (
    total_outliers / total_pedidos_pb
) * 100

percentual_valor = (
    valor_outliers / valor_total_pb
) * 100

print("=== IMPACTO DOS OUTLIERS - PARAÍBA ===")

print(f"Total de pedidos: {total_pedidos_pb}")
print(f"Pedidos acima do limite: {total_outliers}")
print(f"Percentual de pedidos: {percentual_pedidos:.2f}%")
print(f"Valor dos pedidos acima do limite: R$ {valor_outliers:.2f}")
print(f"Participação no valor total: {percentual_valor:.2f}%")

# COMMAND ----------

# ==========================================================
# ETAPA 8.14 - TICKET MÉDIO COM E SEM OUTLIERS
# ==========================================================

df_pedidos_pb_sem_outliers = (
    df_pedidos_pb
    .filter(F.col("valor_pedido") <= limite_superior)
)

ticket_com_outliers = df_pedidos_pb.agg(
    F.avg("valor_pedido")
).first()[0]

ticket_sem_outliers = df_pedidos_pb_sem_outliers.agg(
    F.avg("valor_pedido")
).first()[0]

reducao_percentual = (
    (ticket_com_outliers - ticket_sem_outliers)
    / ticket_com_outliers
) * 100

print("=== COMPARAÇÃO DE TICKET MÉDIO ===")

print(f"Ticket médio original: R$ {ticket_com_outliers:.2f}")
print(f"Ticket médio sem outliers: R$ {ticket_sem_outliers:.2f}")
print(f"Redução percentual: {reducao_percentual:.2f}%")
print(f"Pedidos restantes: {df_pedidos_pb_sem_outliers.count()}")

# COMMAND ----------

# ==========================================================
# ETAPA 8.15 - COMPARAÇÃO ENTRE SP E PB
# ==========================================================

df_pedidos_sp_pb = (
    df_vendas_clientes
    .filter(F.col("customer_state").isin("SP", "PB"))
    .groupBy("customer_state", "order_id")
    .agg(
        F.round(F.sum("price"), 2).alias("valor_pedido")
    )
)

df_comparacao_sp_pb = (
    df_pedidos_sp_pb
    .groupBy("customer_state")
    .agg(
        F.count("*").alias("total_pedidos"),
        F.round(F.avg("valor_pedido"), 2).alias("media"),
        F.round(F.median("valor_pedido"), 2).alias("mediana")
    )
    .orderBy("customer_state")
)

print("=== COMPARAÇÃO ENTRE SP E PB ===")

df_comparacao_sp_pb.show(truncate=False)

# COMMAND ----------

# ==========================================================
# ETAPA 9.1 - CARREGAMENTO DA TABELA DE PAGAMENTOS
# ==========================================================

df_pagamentos = (
    spark.read
    .format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load(f"{caminho_base}/olist_order_payments_dataset.csv")
)

print("=== TABELA DE PAGAMENTOS ===")

print(f"Total de registros: {df_pagamentos.count()}")

print("\n=== ESTRUTURA DA TABELA ===")

df_pagamentos.printSchema()

print("\n=== PRIMEIROS REGISTROS ===")

display(df_pagamentos.limit(10))

# COMMAND ----------

# ==========================================================
# ETAPA 9.2 - VALIDAÇÃO DOS REGISTROS DE PAGAMENTO
# ==========================================================

total_registros = df_pagamentos.count()

total_pedidos = (
    df_pagamentos
    .select("order_id")
    .distinct()
    .count()
)

registros_adicionais = total_registros - total_pedidos

print("=== VALIDAÇÃO DOS PAGAMENTOS ===")

print(f"Total de registros: {total_registros}")
print(f"Pedidos distintos: {total_pedidos}")
print(f"Registros adicionais: {registros_adicionais}")

# COMMAND ----------

# ==========================================================
# ETAPA 9.3 - PEDIDOS COM MÚLTIPLOS PAGAMENTOS
# ==========================================================

df_pagamentos_por_pedido = (
    df_pagamentos
    .groupBy("order_id")
    .agg(
        F.count("*").alias("quantidade_pagamentos")
    )
)

df_multiplos_pagamentos = (
    df_pagamentos_por_pedido
    .filter(F.col("quantidade_pagamentos") > 1)
)

print("=== ANÁLISE DE MÚLTIPLOS PAGAMENTOS ===")

print(
    f"Pedidos com múltiplos pagamentos: "
    f"{df_multiplos_pagamentos.count()}"
)

print("\n=== DISTRIBUIÇÃO DA QUANTIDADE DE PAGAMENTOS ===")

(
    df_pagamentos_por_pedido
    .groupBy("quantidade_pagamentos")
    .agg(
        F.count("*").alias("total_pedidos")
    )
    .orderBy("quantidade_pagamentos")
    .show(truncate=False)
)

# COMMAND ----------

# ==========================================================
# ETAPA 9.4 - PEDIDOS SEM REGISTROS DE PAGAMENTO
# ==========================================================

df_pedidos_sem_pagamento = (
    df_pedidos
    .join(
        df_pagamentos.select("order_id").distinct(),
        on="order_id",
        how="left_anti"
    )
)

print("=== PEDIDOS SEM REGISTRO DE PAGAMENTO ===")

print(
    f"Total de pedidos sem pagamento: "
    f"{df_pedidos_sem_pagamento.count()}"
)

df_pedidos_sem_pagamento.select(
    "order_id",
    "order_status",
    "order_purchase_timestamp"
).show(truncate=False)

# COMMAND ----------

# ==========================================================
# ETAPA 9.5 - DISTRIBUIÇÃO DOS MÉTODOS DE PAGAMENTO
# ==========================================================

df_metodos_pagamento = (
    df_pagamentos
    .groupBy("payment_type")
    .agg(
        F.count("*").alias("total_registros"),
        F.countDistinct("order_id").alias("total_pedidos")
    )
    .orderBy(F.desc("total_registros"))
)

print("=== MÉTODOS DE PAGAMENTO DA OLIST ===")

df_metodos_pagamento.show(truncate=False)

# COMMAND ----------

# ==========================================================
# ETAPA 9.6 - PARTICIPAÇÃO DOS MÉTODOS DE PAGAMENTO
# ==========================================================

total_pagamentos = df_pagamentos.count()

df_participacao_pagamentos = (
    df_metodos_pagamento
    .withColumn(
        "participacao_percentual",
        F.round(
            F.col("total_registros") /
            F.lit(total_pagamentos) * 100,
            2
        )
    )
    .orderBy(F.desc("total_registros"))
)

print("=== PARTICIPAÇÃO DOS MÉTODOS DE PAGAMENTO ===")

df_participacao_pagamentos.show(truncate=False)

# COMMAND ----------

# ==========================================================
# ETAPA 9.7 - VALOR POR MÉTODO DE PAGAMENTO
# ==========================================================

df_valor_metodos = (
    df_pagamentos
    .groupBy("payment_type")
    .agg(
        F.round(
            F.sum("payment_value"), 2
        ).alias("valor_total_pagamentos"),

        F.count("*").alias("total_registros")
    )
    .orderBy(F.desc("valor_total_pagamentos"))
)

print("=== VALOR MOVIMENTADO POR MÉTODO ===")

df_valor_metodos.show(truncate=False)

# COMMAND ----------

# ==========================================================
# ETAPA 9.8 - PARTICIPAÇÃO NO VALOR DOS PAGAMENTOS
# ==========================================================

total_valor_pagamentos = (
    df_pagamentos
    .agg(F.sum("payment_value"))
    .first()[0]
)

df_participacao_valor = (
    df_valor_metodos
    .withColumn(
        "participacao_valor_percentual",
        F.round(
            F.col("valor_total_pagamentos") /
            F.lit(total_valor_pagamentos) * 100,
            2
        )
    )
    .orderBy(F.desc("valor_total_pagamentos"))
)

print("=== PARTICIPAÇÃO NO VALOR DOS PAGAMENTOS ===")

df_participacao_valor.show(truncate=False)

# COMMAND ----------

# ==========================================================
# ETAPA 9.9 - VALOR MÉDIO POR REGISTRO DE PAGAMENTO
# ==========================================================

df_media_pagamentos = (
    df_pagamentos
    .groupBy("payment_type")
    .agg(
        F.count("*").alias("total_registros"),

        F.round(
            F.avg("payment_value"), 2
        ).alias("valor_medio_pagamento")
    )
    .orderBy(F.desc("valor_medio_pagamento"))
)

print("=== VALOR MÉDIO POR REGISTRO DE PAGAMENTO ===")

df_media_pagamentos.show(truncate=False)

# COMMAND ----------

# ==========================================================
# ETAPA 9.10 - DISTRIBUIÇÃO DOS PARCELAMENTOS
# ==========================================================

df_parcelamentos = (
    df_pagamentos
    .filter(F.col("payment_type") == "credit_card")
    .groupBy("payment_installments")
    .agg(
        F.count("*").alias("total_registros")
    )
    .orderBy(F.desc("total_registros"))
)

print("=== PARCELAMENTOS NO CARTÃO DE CRÉDITO ===")

df_parcelamentos.show(20, truncate=False)

# COMMAND ----------

# ==========================================================
# ETAPA 9.11 - CLASSIFICAÇÃO DOS PARCELAMENTOS
# ==========================================================

df_tipo_parcelamento = (
    df_pagamentos
    .filter(F.col("payment_type") == "credit_card")
    .withColumn(
        "tipo_parcelamento",
        F.when(
            F.col("payment_installments") == 1,
            "À vista"
        )
        .when(
            F.col("payment_installments") >= 2,
            "Parcelado"
        )
        .otherwise("Inconsistente")
    )
)

df_resumo_parcelamentos = (
    df_tipo_parcelamento
    .groupBy("tipo_parcelamento")
    .agg(
        F.count("*").alias("total_registros")
    )
    .withColumn(
        "percentual",
        F.round(
            F.col("total_registros") /
            F.lit(76795) * 100,
            2
        )
    )
    .orderBy(F.desc("total_registros"))
)

print("=== RESUMO DOS PARCELAMENTOS ===")

df_resumo_parcelamentos.show(truncate=False)

# COMMAND ----------

# ==========================================================
# ETAPA 9.12 - VALOR MÉDIO POR QUANTIDADE DE PARCELAS
# ==========================================================

df_valor_por_parcelas = (
    df_pagamentos
    .filter(
        (F.col("payment_type") == "credit_card") &
        (F.col("payment_installments") >= 1)
    )
    .groupBy("payment_installments")
    .agg(
        F.count("*").alias("total_registros"),

        F.round(
            F.avg("payment_value"), 2
        ).alias("valor_medio")
    )
    .orderBy("payment_installments")
)

print("=== VALOR MÉDIO POR QUANTIDADE DE PARCELAS ===")

df_valor_por_parcelas.show(15, truncate=False)

# COMMAND ----------

# ==========================================================
# ETAPA 9.13 - VALOR MÉDIO À VISTA VS. PARCELADO
# ==========================================================

df_comparacao_parcelamento = (
    df_tipo_parcelamento
    .filter(F.col("tipo_parcelamento") != "Inconsistente")
    .groupBy("tipo_parcelamento")
    .agg(
        F.count("*").alias("total_registros"),

        F.round(
            F.avg("payment_value"), 2
        ).alias("valor_medio"),

        F.round(
            F.sum("payment_value"), 2
        ).alias("valor_total")
    )
    .orderBy(F.desc("valor_medio"))
)

print("=== COMPARAÇÃO: À VISTA VS. PARCELADO ===")

df_comparacao_parcelamento.show(truncate=False)

# COMMAND ----------

# ==========================================================
# ETAPA 9.14 - DIFERENÇA PERCENTUAL ENTRE AS MÉDIAS
# ==========================================================

media_vista = (
    df_comparacao_parcelamento
    .filter(F.col("tipo_parcelamento") == "À vista")
    .select("valor_medio")
    .first()[0]
)

media_parcelado = (
    df_comparacao_parcelamento
    .filter(F.col("tipo_parcelamento") == "Parcelado")
    .select("valor_medio")
    .first()[0]
)

diferenca_percentual = (
    (media_parcelado / media_vista) - 1
) * 100

print("=== COMPARAÇÃO ENTRE AS MÉDIAS ===")

print(f"Média à vista: R$ {media_vista:.2f}")
print(f"Média parcelada: R$ {media_parcelado:.2f}")
print(f"Diferença percentual: {diferenca_percentual:.2f}%")

# COMMAND ----------

# ==========================================================
# ETAPA 10.1 - CARREGAMENTO DAS AVALIAÇÕES
# ==========================================================

df_avaliacoes = (
    spark.read
    .format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load(f"{caminho_base}/olist_order_reviews_dataset.csv")
)

print("=== TABELA DE AVALIAÇÕES ===")

print(f"Total de registros: {df_avaliacoes.count()}")

print("\n=== ESTRUTURA DA TABELA ===")

df_avaliacoes.printSchema()

print("\n=== PRIMEIROS REGISTROS ===")

display(df_avaliacoes.limit(10))

# COMMAND ----------

# ==========================================================
# ETAPA 10.2 - VALIDAÇÃO DAS AVALIAÇÕES
# ==========================================================

total_avaliacoes = df_avaliacoes.count()

pedidos_distintos = (
    df_avaliacoes
    .select("order_id")
    .distinct()
    .count()
)

registros_adicionais = (
    total_avaliacoes - pedidos_distintos
)

print("=== VALIDAÇÃO DAS AVALIAÇÕES ===")

print(f"Total de avaliações: {total_avaliacoes}")
print(f"Pedidos distintos: {pedidos_distintos}")
print(f"Registros adicionais: {registros_adicionais}")

# COMMAND ----------

# ==========================================================
# ETAPA 10.3 - PEDIDOS COM MÚLTIPLAS AVALIAÇÕES
# ==========================================================

df_avaliacoes_por_pedido = (
    df_avaliacoes
    .groupBy("order_id")
    .agg(
        F.count("*").alias("quantidade_avaliacoes")
    )
)

df_multiplas_avaliacoes = (
    df_avaliacoes_por_pedido
    .filter(F.col("quantidade_avaliacoes") > 1)
)

print("=== PEDIDOS COM MÚLTIPLAS AVALIAÇÕES ===")

print(
    f"Pedidos com múltiplas avaliações: "
    f"{df_multiplas_avaliacoes.count()}"
)

print("\n=== DISTRIBUIÇÃO DAS AVALIAÇÕES POR PEDIDO ===")

(
    df_avaliacoes_por_pedido
    .groupBy("quantidade_avaliacoes")
    .agg(
        F.count("*").alias("total_pedidos")
    )
    .orderBy("quantidade_avaliacoes")
    .show(truncate=False)
)

# COMMAND ----------

# ==========================================================
# ETAPA 10.4 - VERIFICAÇÃO DE ORDER_ID NULOS
# ==========================================================

total_nulos = (
    df_avaliacoes
    .filter(F.col("order_id").isNull())
    .count()
)

total_nao_nulos = (
    df_avaliacoes
    .filter(F.col("order_id").isNotNull())
    .count()
)

print("=== VALIDAÇÃO DA CHAVE ORDER_ID ===")

print(f"Total de registros: {df_avaliacoes.count()}")
print(f"order_id nulos: {total_nulos}")
print(f"order_id não nulos: {total_nao_nulos}")

# COMMAND ----------

# ==========================================================
# ETAPA 10.5 - AVALIAÇÕES COM ORDER_ID PREENCHIDO
# ==========================================================

df_avaliacoes_validas = (
    df_avaliacoes
    .filter(F.col("order_id").isNotNull())
)

total_registros = df_avaliacoes_validas.count()

pedidos_distintos = (
    df_avaliacoes_validas
    .select("order_id")
    .distinct()
    .count()
)

registros_adicionais = total_registros - pedidos_distintos

print("=== VALIDAÇÃO DAS AVALIAÇÕES COM CHAVE PREENCHIDA ===")

print(f"Total de registros: {total_registros}")
print(f"Pedidos distintos: {pedidos_distintos}")
print(f"Registros adicionais: {registros_adicionais}")

# COMMAND ----------

# ==========================================================
# ETAPA 10.6 - AVALIAÇÕES SEM PEDIDO CORRESPONDENTE
# ==========================================================

df_avaliacoes_sem_pedido = (
    df_avaliacoes_validas
    .select("order_id")
    .distinct()
    .join(
        df_pedidos.select("order_id").distinct(),
        on="order_id",
        how="left_anti"
    )
)

print("=== INTEGRIDADE ENTRE AVALIAÇÕES E PEDIDOS ===")

print(
    f"Pedidos distintos nas avaliações: "
    f"{df_avaliacoes_validas.select('order_id').distinct().count()}"
)

print(
    f"Identificadores sem correspondência: "
    f"{df_avaliacoes_sem_pedido.count()}"
)

# COMMAND ----------

# ==========================================================
# ETAPA 10.7 - AVALIAÇÕES COM PEDIDOS CORRESPONDENTES
# ==========================================================

df_avaliacoes_correspondentes = (
    df_avaliacoes_validas
    .join(
        df_pedidos.select("order_id").distinct(),
        on="order_id",
        how="left_semi"
    )
)

print("=== AVALIAÇÕES COM PEDIDOS CORRESPONDENTES ===")

print(
    f"Total de registros: "
    f"{df_avaliacoes_correspondentes.count()}"
)

print(
    f"Pedidos distintos: "
    f"{df_avaliacoes_correspondentes.select('order_id').distinct().count()}"
)

# COMMAND ----------

# ==========================================================
# ETAPA 10.8 - MÚLTIPLAS AVALIAÇÕES POR PEDIDO
# ==========================================================

df_contagem_avaliacoes = (
    df_avaliacoes_correspondentes
    .groupBy("order_id")
    .agg(
        F.count("*").alias("quantidade_avaliacoes")
    )
)

df_pedidos_multiplas_avaliacoes = (
    df_contagem_avaliacoes
    .filter(F.col("quantidade_avaliacoes") > 1)
)

print("=== ANÁLISE DE MÚLTIPLAS AVALIAÇÕES ===")

print(
    f"Pedidos com múltiplas avaliações: "
    f"{df_pedidos_multiplas_avaliacoes.count()}"
)

print("\n=== DISTRIBUIÇÃO DAS AVALIAÇÕES POR PEDIDO ===")

(
    df_contagem_avaliacoes
    .groupBy("quantidade_avaliacoes")
    .agg(
        F.count("*").alias("total_pedidos")
    )
    .orderBy("quantidade_avaliacoes")
    .show(truncate=False)
)

# COMMAND ----------

# ==========================================================
# ETAPA 10.9 - DIVERGÊNCIAS NAS NOTAS DAS AVALIAÇÕES
# ==========================================================

df_divergencias_notas = (
    df_avaliacoes_correspondentes
    .groupBy("order_id")
    .agg(
        F.count("*").alias("total_avaliacoes"),
        F.countDistinct("review_score").alias("notas_distintas")
    )
    .filter(F.col("total_avaliacoes") > 1)
)

print("=== ANÁLISE DE DIVERGÊNCIAS NAS NOTAS ===")

print(
    "Pedidos com múltiplas avaliações:",
    df_divergencias_notas.count()
)

print(
    "Pedidos com notas diferentes:",
    df_divergencias_notas
    .filter(F.col("notas_distintas") > 1)
    .count()
)

print(
    "Pedidos com uma única nota distinta:",
    df_divergencias_notas
    .filter(F.col("notas_distintas") == 1)
    .count()
)

# COMMAND ----------

# ==========================================================
# ETAPA 10.10 - INVESTIGAR A SEQUÊNCIA DAS AVALIAÇÕES
# ==========================================================

df_pedidos_notas_diferentes = (
    df_divergencias_notas
    .filter(F.col("notas_distintas") > 1)
    .select("order_id")
)

df_exemplos_divergencias = (
    df_avaliacoes_correspondentes
    .join(
        df_pedidos_notas_diferentes,
        on="order_id",
        how="inner"
    )
    .select(
        "order_id",
        "review_score",
        "review_creation_date"
    )
    .orderBy(
        "order_id",
        "review_creation_date"
    )
)

display(df_exemplos_divergencias.limit(20))

# COMMAND ----------

# ==========================================================
# ETAPA 10.11 - QUALIDADE DAS DATAS DAS AVALIAÇÕES
# ==========================================================

df_qualidade_datas = (
    df_avaliacoes_correspondentes
    .agg(
        F.count("*").alias("total_avaliacoes"),

        F.sum(
            F.when(
                F.col("review_creation_date").isNull(), 1
            ).otherwise(0)
        ).alias("datas_nulas"),

        F.countDistinct(
            "review_creation_date"
        ).alias("datas_distintas")
    )
)

display(df_qualidade_datas)

# COMMAND ----------

# ==========================================================
# ETAPA 10.12 - DATAS NULAS EM AVALIAÇÕES MÚLTIPLAS
# ==========================================================

df_qualidade_multiplas = (
    df_avaliacoes_correspondentes
    .join(
        df_pedidos_multiplas_avaliacoes.select("order_id"),
        on="order_id",
        how="inner"
    )
    .groupBy("order_id")
    .agg(
        F.count("*").alias("total_avaliacoes"),

        F.sum(
            F.when(
                F.col("review_creation_date").isNull(), 1
            ).otherwise(0)
        ).alias("datas_nulas"),

        F.countDistinct(
            "review_creation_date"
        ).alias("datas_distintas")
    )
)

print("=== QUALIDADE DAS DATAS EM PEDIDOS COM MÚLTIPLAS AVALIAÇÕES ===")

print(
    "Pedidos com múltiplas avaliações:",
    df_qualidade_multiplas.count()
)

print(
    "Pedidos com pelo menos uma data nula:",
    df_qualidade_multiplas
    .filter(F.col("datas_nulas") > 0)
    .count()
)

print(
    "Pedidos com todas as datas preenchidas:",
    df_qualidade_multiplas
    .filter(F.col("datas_nulas") == 0)
    .count()
)

print(
    "Pedidos com apenas uma data distinta:",
    df_qualidade_multiplas
    .filter(F.col("datas_distintas") == 1)
    .count()
)

# COMMAND ----------

# ==========================================================
# ETAPA 10.13 - EMPATES NA DATA MAIS RECENTE
# ==========================================================

df_empates_datas = (
    df_avaliacoes_correspondentes
    .join(
        df_pedidos_multiplas_avaliacoes.select("order_id"),
        on="order_id",
        how="inner"
    )
    .withColumn(
        "ultima_data",
        F.max("review_creation_date").over(
            Window.partitionBy("order_id")
        )
    )
    .filter(
        F.col("review_creation_date") == F.col("ultima_data")
    )
    .groupBy("order_id")
    .agg(
        F.count("*").alias("avaliacoes_na_ultima_data")
    )
)

print("=== EMPATES NA DATA MAIS RECENTE ===")

print(
    "Pedidos com empate na última data:",
    df_empates_datas
    .filter(F.col("avaliacoes_na_ultima_data") > 1)
    .count()
)

print(
    "Pedidos com uma única avaliação na última data:",
    df_empates_datas
    .filter(F.col("avaliacoes_na_ultima_data") == 1)
    .count()
)

# COMMAND ----------

# ==========================================================
# ETAPA 10.14 - NOTAS DIFERENTES NA ÚLTIMA DATA
# ==========================================================

df_notas_ultima_data = (
    df_avaliacoes_correspondentes
    .join(
        df_pedidos_multiplas_avaliacoes.select("order_id"),
        on="order_id",
        how="inner"
    )
    .withColumn(
        "ultima_data",
        F.max("review_creation_date").over(
            Window.partitionBy("order_id")
        )
    )
    .filter(
        F.col("review_creation_date") == F.col("ultima_data")
    )
    .groupBy("order_id")
    .agg(
        F.count("*").alias("total_avaliacoes"),
        F.countDistinct("review_score").alias("notas_distintas")
    )
    .filter(F.col("total_avaliacoes") > 1)
)

print("=== DIVERGÊNCIAS NAS DATAS EMPATADAS ===")

print(
    "Pedidos com empate na última data:",
    df_notas_ultima_data.count()
)

print(
    "Pedidos com notas diferentes:",
    df_notas_ultima_data
    .filter(F.col("notas_distintas") > 1)
    .count()
)

print(
    "Pedidos com notas iguais:",
    df_notas_ultima_data
    .filter(F.col("notas_distintas") == 1)
    .count()
)

# COMMAND ----------

# ==========================================================
# ETAPA 10.15 - INSPECIONAR COLUNAS E TIPOS DAS AVALIAÇÕES
# ==========================================================

print("=== ESTRUTURA DA TABELA DE AVALIAÇÕES ===")

df_avaliacoes_correspondentes.printSchema()

# COMMAND ----------

# ==========================================================
# ETAPA 10.16 - CONVERSÃO DAS COLUNAS TEMPORAIS
# ==========================================================

df_avaliacoes_datas = (
    df_avaliacoes_correspondentes
    .withColumn(
        "data_criacao",
        F.to_date("review_creation_date")
    )
    .withColumn(
        "data_resposta",
        F.to_timestamp("review_answer_timestamp")
    )
)

print("=== NOVA ESTRUTURA DAS AVALIAÇÕES ===")

df_avaliacoes_datas.select(
    "order_id",
    "review_score",
    "data_criacao",
    "data_resposta"
).printSchema()

print("\n=== EXEMPLOS DE DATAS CONVERTIDAS ===")

display(
    df_avaliacoes_datas.select(
        "order_id",
        "review_score",
        "data_criacao",
        "data_resposta"
    ).limit(10)
)

# COMMAND ----------

# ==========================================================
# ETAPA 10.17 - REIMPORTAR O CSV DE AVALIAÇÕES
# ==========================================================

df_avaliacoes_corrigido = (
    spark.read
    .format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .option("multiLine", True)
    .option("quote", '"')
    .option("escape", '"')
    .load(
        f"{caminho_base}/olist_order_reviews_dataset.csv"
    )
)

print("=== VALIDAÇÃO DA NOVA IMPORTAÇÃO ===")

print(
    "Total de registros:",
    df_avaliacoes_corrigido.count()
)

print(
    "Order IDs nulos:",
    df_avaliacoes_corrigido
    .filter(F.col("order_id").isNull())
    .count()
)

print(
    "Pedidos distintos:",
    df_avaliacoes_corrigido
    .select("order_id")
    .distinct()
    .count()
)

# COMMAND ----------

# ==========================================================
# ETAPA 10.18 - ATUALIZAR A BASE DE AVALIAÇÕES
# ==========================================================

# Substituir a base importada incorretamente
df_avaliacoes = df_avaliacoes_corrigido

print("=== ESTRUTURA DA BASE CORRIGIDA ===")

df_avaliacoes.printSchema()

print("\n=== TOTAL DE REGISTROS ===")

print(
    "Total de avaliações:",
    df_avaliacoes.count()
)

# COMMAND ----------

# ==========================================================
# ETAPA 10.19 - INTEGRIDADE DA BASE CORRIGIDA
# ==========================================================

df_avaliacoes_sem_correspondencia = (
    df_avaliacoes
    .select("order_id")
    .distinct()
    .join(
        df_pedidos.select("order_id").distinct(),
        on="order_id",
        how="left_anti"
    )
)

print("=== VALIDAÇÃO DA BASE CORRIGIDA ===")

print(
    "Total de avaliações:",
    df_avaliacoes.count()
)

print(
    "Pedidos distintos nas avaliações:",
    df_avaliacoes.select("order_id").distinct().count()
)

print(
    "Pedidos sem correspondência:",
    df_avaliacoes_sem_correspondencia.count()
)

# COMMAND ----------

# ==========================================================
# ETAPA 10.20 - AVALIAÇÕES MÚLTIPLAS NA BASE CORRIGIDA
# ==========================================================

df_contagem_avaliacoes = (
    df_avaliacoes
    .groupBy("order_id")
    .agg(
        F.count("*").alias("quantidade_avaliacoes"),
        F.countDistinct("review_score").alias("notas_distintas")
    )
)

print("=== VALIDAÇÃO DAS AVALIAÇÕES MÚLTIPLAS ===")

print(
    "Pedidos com múltiplas avaliações:",
    df_contagem_avaliacoes
    .filter(F.col("quantidade_avaliacoes") > 1)
    .count()
)

print(
    "Pedidos com múltiplas avaliações e notas diferentes:",
    df_contagem_avaliacoes
    .filter(
        (F.col("quantidade_avaliacoes") > 1) &
        (F.col("notas_distintas") > 1)
    )
    .count()
)

print("\n=== DISTRIBUIÇÃO DAS AVALIAÇÕES POR PEDIDO ===")

display(
    df_contagem_avaliacoes
    .groupBy("quantidade_avaliacoes")
    .agg(F.count("*").alias("total_pedidos"))
    .orderBy("quantidade_avaliacoes")
)

# COMMAND ----------

# ==========================================================
# ETAPA 10.21 - CONSOLIDAR AVALIAÇÕES POR PEDIDO
# ==========================================================

df_satisfacao_pedido = (
    df_avaliacoes
    .groupBy("order_id")
    .agg(
        F.count("*").alias("total_avaliacoes"),
        F.avg("review_score").alias("nota_media_pedido"),
        F.min("review_score").alias("menor_nota"),
        F.max("review_score").alias("maior_nota")
    )
)

print("=== VALIDAÇÃO DA SATISFAÇÃO POR PEDIDO ===")

print(
    "Total de pedidos consolidados:",
    df_satisfacao_pedido.count()
)

print(
    "Pedidos distintos:",
    df_satisfacao_pedido
    .select("order_id")
    .distinct()
    .count()
)

print("\n=== EXEMPLOS ===")

display(
    df_satisfacao_pedido
    .filter(F.col("total_avaliacoes") > 1)
    .orderBy(F.desc("total_avaliacoes"))
    .limit(10)
)

# COMMAND ----------

# ==========================================================
# ETAPA 10.22 - INDICADORES GERAIS DE SATISFAÇÃO
# ==========================================================

df_indicadores_satisfacao = (
    df_satisfacao_pedido
    .agg(
        F.count("*").alias("pedidos_avaliados"),

        F.round(
            F.avg("nota_media_pedido"), 2
        ).alias("nota_media_geral"),

        F.round(
            F.avg(
                F.when(
                    F.col("nota_media_pedido") >= 4, 1
                ).otherwise(0)
            ) * 100, 2
        ).alias("percentual_positivos"),

        F.round(
            F.avg(
                F.when(
                    F.col("nota_media_pedido") <= 2, 1
                ).otherwise(0)
            ) * 100, 2
        ).alias("percentual_negativos")
    )
)

display(df_indicadores_satisfacao)

# COMMAND ----------

# ==========================================================
# ETAPA 10.23 - DISTRIBUIÇÃO DAS NOTAS POR PEDIDO
# ==========================================================

df_distribuicao_satisfacao = (
    df_satisfacao_pedido
    .groupBy("nota_media_pedido")
    .agg(
        F.count("*").alias("total_pedidos")
    )
    .withColumn(
        "percentual",
        F.round(
            F.col("total_pedidos") / 98673 * 100,
            2
        )
    )
    .orderBy("nota_media_pedido")
)

display(df_distribuicao_satisfacao)

# COMMAND ----------

# ==========================================================
# ETAPA 11.1 - QUALIDADE DOS DADOS DE ENTREGA
# ==========================================================

df_qualidade_entregas = (
    df_pedidos
    .filter(F.col("order_status") == "delivered")
    .agg(
        F.count("*").alias("pedidos_entregues"),

        F.sum(
            F.when(
                F.col("order_delivered_customer_date").isNull(), 1
            ).otherwise(0)
        ).alias("entrega_real_nula"),

        F.sum(
            F.when(
                F.col("order_estimated_delivery_date").isNull(), 1
            ).otherwise(0)
        ).alias("previsao_entrega_nula")
    )
)

display(df_qualidade_entregas)

# COMMAND ----------

# ==========================================================
# ETAPA 11.2 - IDENTIFICAR ATRASOS NAS ENTREGAS
# ==========================================================

df_entregas_analise = (
    df_pedidos
    .filter(
        (F.col("order_status") == "delivered") &
        (F.col("order_delivered_customer_date").isNotNull()) &
        (F.col("order_estimated_delivery_date").isNotNull())
    )
    .withColumn(
        "dias_atraso",
        F.datediff(
            F.to_date("order_delivered_customer_date"),
            F.to_date("order_estimated_delivery_date")
        )
    )
    .withColumn(
        "situacao_entrega",
        F.when(
            F.col("dias_atraso") > 0,
            "Atrasado"
        ).otherwise("No prazo")
    )
)

print("=== VALIDAÇÃO DA BASE DE ENTREGAS ===")

print("Total de pedidos analisados:", df_entregas_analise.count())

print("\n=== DISTRIBUIÇÃO DAS ENTREGAS ===")

display(
    df_entregas_analise
    .groupBy("situacao_entrega")
    .agg(
        F.count("*").alias("total_pedidos")
    )
    .orderBy("situacao_entrega")
)

# COMMAND ----------

# ==========================================================
# ETAPA 11.3 - DURAÇÃO DOS ATRASOS
# ==========================================================

df_duracao_atrasos = (
    df_entregas_analise
    .filter(F.col("dias_atraso") > 0)
    .agg(
        F.count("*").alias("pedidos_atrasados"),

        F.round(
            F.avg("dias_atraso"), 2
        ).alias("media_dias_atraso"),

        F.expr(
            "percentile_approx(dias_atraso, 0.5)"
        ).alias("mediana_dias_atraso"),

        F.max("dias_atraso").alias("maior_atraso")
    )
)

display(df_duracao_atrasos)

# COMMAND ----------

# ==========================================================
# ETAPA 11.4 - CLASSIFICAÇÃO DA GRAVIDADE DOS ATRASOS
# ==========================================================

df_faixas_atraso = (
    df_entregas_analise
    .filter(F.col("dias_atraso") > 0)
    .withColumn(
        "faixa_atraso",
        F.when(F.col("dias_atraso") <= 3, "1 a 3 dias")
         .when(F.col("dias_atraso") <= 7, "4 a 7 dias")
         .when(F.col("dias_atraso") <= 15, "8 a 15 dias")
         .otherwise("Acima de 15 dias")
    )
)

df_distribuicao_atrasos = (
    df_faixas_atraso
    .groupBy("faixa_atraso")
    .agg(F.count("*").alias("total_pedidos"))
    .withColumn(
        "percentual",
        F.round(
            F.col("total_pedidos") / 6534 * 100, 2
        )
    )
)

display(df_distribuicao_atrasos)

# COMMAND ----------

# ==========================================================
# ETAPA 11.5 - RELACIONAR ENTREGAS E SATISFAÇÃO
# ==========================================================

df_entregas_satisfacao = (
    df_entregas_analise
    .join(
        df_satisfacao_pedido.select(
            "order_id",
            "nota_media_pedido"
        ),
        on="order_id",
        how="inner"
    )
)

print("=== VALIDAÇÃO DO RELACIONAMENTO ===")

print(
    "Pedidos entregues com datas válidas:",
    df_entregas_analise.count()
)

print(
    "Pedidos entregues com avaliação:",
    df_entregas_satisfacao.count()
)

print(
    "Pedidos distintos após o JOIN:",
    df_entregas_satisfacao
    .select("order_id")
    .distinct()
    .count()
)

# COMMAND ----------

# ==========================================================
# ETAPA 11.6 - SATISFAÇÃO POR SITUAÇÃO DA ENTREGA
# ==========================================================

df_satisfacao_por_entrega = (
    df_entregas_satisfacao
    .groupBy("situacao_entrega")
    .agg(
        F.count("*").alias("total_pedidos"),

        F.round(
            F.avg("nota_media_pedido"), 2
        ).alias("nota_media"),

        F.round(
            F.avg(
                F.when(
                    F.col("nota_media_pedido") <= 2, 1
                ).otherwise(0)
            ) * 100, 2
        ).alias("percentual_negativas")
    )
)

display(df_satisfacao_por_entrega)

# COMMAND ----------

# ==========================================================
# ETAPA 11.7 - SATISFAÇÃO POR GRAVIDADE DO ATRASO
# ==========================================================

df_satisfacao_faixa_atraso = (
    df_entregas_satisfacao
    .filter(F.col("dias_atraso") > 0)
    .withColumn(
        "faixa_atraso",
        F.when(F.col("dias_atraso") <= 3, "1 a 3 dias")
         .when(F.col("dias_atraso") <= 7, "4 a 7 dias")
         .when(F.col("dias_atraso") <= 15, "8 a 15 dias")
         .otherwise("Acima de 15 dias")
    )
    .groupBy("faixa_atraso")
    .agg(
        F.count("*").alias("total_pedidos"),

        F.round(
            F.avg("nota_media_pedido"), 2
        ).alias("nota_media"),

        F.round(
            F.avg(
                F.when(
                    F.col("nota_media_pedido") <= 2, 1
                ).otherwise(0)
            ) * 100, 2
        ).alias("percentual_negativas")
    )
)

display(df_satisfacao_faixa_atraso)

# COMMAND ----------

# ==========================================================
# ETAPA 11.8 - CORRELAÇÃO ENTRE ATRASO E SATISFAÇÃO
# ==========================================================

df_apenas_atrasados = (
    df_entregas_satisfacao
    .filter(F.col("dias_atraso") > 0)
)

correlacao = df_apenas_atrasados.stat.corr(
    "dias_atraso",
    "nota_media_pedido"
)

print("=== CORRELAÇÃO ENTRE ATRASO E SATISFAÇÃO ===")

print(f"Pedidos analisados: {df_apenas_atrasados.count()}")

print(f"Correlação de Pearson: {correlacao:.4f}")

# COMMAND ----------

# ==========================================================
# ETAPA 11.9 - CORRELAÇÃO DE SPEARMAN
# ==========================================================

from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler

df_correlacao = (
    df_apenas_atrasados
    .select(
        F.col("dias_atraso").cast("double"),
        F.col("nota_media_pedido").cast("double")
    )
    .dropna()
)

# Spearman trabalha com a ordenação dos valores.
# Primeiro criamos os postos (ranks) de cada variável.

janela_atraso = Window.orderBy("dias_atraso")
janela_nota = Window.orderBy("nota_media_pedido")

df_ranks = (
    df_correlacao
    .withColumn(
        "rank_atraso",
        F.cume_dist().over(janela_atraso)
    )
    .withColumn(
        "rank_nota",
        F.cume_dist().over(janela_nota)
    )
)

spearman_aproximado = df_ranks.stat.corr(
    "rank_atraso",
    "rank_nota"
)

print("=== COMPARAÇÃO DAS CORRELAÇÕES ===")
print("Pearson:", round(correlacao, 4))
print("Spearman aproximado:", round(spearman_aproximado, 4))

# COMMAND ----------

# ==========================================================
# ETAPA 11.10 - CORRELAÇÃO DE SPEARMAN COM PYSPARK MLLIB
# ==========================================================

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation

df_spearman = (
    df_apenas_atrasados
    .select(
        F.col("dias_atraso").cast("double"),
        F.col("nota_media_pedido").cast("double")
    )
    .dropna()
)

assembler = VectorAssembler(
    inputCols=["dias_atraso", "nota_media_pedido"],
    outputCol="features"
)

df_vetores = assembler.transform(df_spearman)

matriz = Correlation.corr(
    df_vetores,
    "features",
    "spearman"
).head()[0]

print("=== CORRELAÇÃO DE SPEARMAN ===")
print(f"Pedidos analisados: {df_spearman.count()}")
print(f"Spearman: {matriz[0, 1]:.4f}")

# COMMAND ----------

# ==========================================================
# ETAPA 11.11 - ATRASOS POR ESTADO
# ==========================================================

df_entregas_estado = (
    df_entregas_analise
    .join(
        df_clientes.select(
            "customer_id",
            "customer_state"
        ),
        on="customer_id",
        how="inner"
    )
)

print("=== VALIDAÇÃO DO JOIN ===")

print(
    "Pedidos antes do JOIN:",
    df_entregas_analise.count()
)

print(
    "Pedidos depois do JOIN:",
    df_entregas_estado.count()
)

print(
    "Pedidos distintos depois do JOIN:",
    df_entregas_estado
    .select("order_id")
    .distinct()
    .count()
)

# COMMAND ----------

# ==========================================================
# ETAPA 11.12 - TAXA DE ATRASO POR ESTADO
# ==========================================================

df_atrasos_estado = (
    df_entregas_estado
    .groupBy("customer_state")
    .agg(
        F.count("*").alias("total_pedidos"),

        F.sum(
            F.when(
                F.col("dias_atraso") > 0, 1
            ).otherwise(0)
        ).alias("pedidos_atrasados")
    )
    .withColumn(
        "taxa_atraso",
        F.round(
            F.col("pedidos_atrasados") /
            F.col("total_pedidos") * 100,
            2
        )
    )
    .orderBy(F.desc("taxa_atraso"))
)

display(df_atrasos_estado)

# COMMAND ----------

# ==========================================================
# ETAPA 11.13 - SATISFAÇÃO E ATRASOS POR ESTADO
# ==========================================================

df_satisfacao_estado = (
    df_entregas_satisfacao
    .join(
        df_clientes.select(
            "customer_id",
            "customer_state"
        ),
        on="customer_id",
        how="inner"
    )
    .groupBy("customer_state")
    .agg(
        F.count("*").alias("pedidos_avaliados"),

        F.round(
            F.avg("nota_media_pedido"), 2
        ).alias("nota_media"),

        F.round(
            F.avg(
                F.when(
                    F.col("dias_atraso") > 0, 1
                ).otherwise(0)
            ) * 100, 2
        ).alias("taxa_atraso"),

        F.round(
            F.avg(
                F.when(
                    F.col("nota_media_pedido") <= 2, 1
                ).otherwise(0)
            ) * 100, 2
        ).alias("percentual_negativas")
    )
    .orderBy(F.desc("taxa_atraso"))
)

display(df_satisfacao_estado)

# COMMAND ----------

# ==========================================================
# ETAPA 11.14 - CORRELAÇÃO ENTRE ESTADOS
# ==========================================================

# Correlação entre taxa de atraso e nota média estadual
correlacao_estados = df_satisfacao_estado.stat.corr(
    "taxa_atraso",
    "nota_media"
)

# Correlação entre taxa de atraso e avaliações negativas
correlacao_negativas = df_satisfacao_estado.stat.corr(
    "taxa_atraso",
    "percentual_negativas"
)

print("=== CORRELAÇÕES POR ESTADO ===")

print(
    "Estados analisados:",
    df_satisfacao_estado.count()
)

print(
    f"Taxa de atraso x Nota média: "
    f"{correlacao_estados:.4f}"
)

print(
    f"Taxa de atraso x Avaliações negativas: "
    f"{correlacao_negativas:.4f}"
)

# COMMAND ----------

# ==========================================================
# ETAPA 11.15 - ESTADOS COM MAIS PEDIDOS ATRASADOS
# ==========================================================

df_top_atrasos = (
    df_atrasos_estado
    .orderBy(
        F.desc("pedidos_atrasados")
    )
    .limit(10)
)

display(df_top_atrasos)

# COMMAND ----------

# ==========================================================
# ETAPA 11.16 - PARTICIPAÇÃO NOS ATRASOS NACIONAIS
# ==========================================================

total_atrasos = (
    df_atrasos_estado
    .agg(
        F.sum("pedidos_atrasados").alias("total")
    )
    .first()["total"]
)

df_impacto_estado = (
    df_atrasos_estado
    .withColumn(
        "participacao_atrasos",
        F.round(
            F.col("pedidos_atrasados") /
            F.lit(total_atrasos) * 100,
            2
        )
    )
    .orderBy(
        F.desc("participacao_atrasos")
    )
)

print("=== IMPACTO DOS ATRASOS POR ESTADO ===")
print(f"Total nacional de atrasos: {total_atrasos}")

display(df_impacto_estado)

# COMMAND ----------

# ==========================================================
# ETAPA 11.17 - MATRIZ DE PRIORIZAÇÃO LOGÍSTICA
# ==========================================================

taxa_nacional = (
    total_atrasos / df_entregas_estado.count()
) * 100

participacao_referencia = 100 / 27

df_matriz_logistica = (
    df_impacto_estado
    .withColumn(
        "classificacao",
        F.when(
            (F.col("taxa_atraso") > taxa_nacional) &
            (F.col("participacao_atrasos") > participacao_referencia),
            "Alta taxa / Alto impacto"
        )
        .when(
            F.col("taxa_atraso") > taxa_nacional,
            "Alta taxa / Baixo impacto"
        )
        .when(
            F.col("participacao_atrasos") > participacao_referencia,
            "Baixa taxa / Alto impacto"
        )
        .otherwise("Baixa taxa / Baixo impacto")
    )
)

print("=== REFERÊNCIAS DA MATRIZ ===")
print(f"Taxa nacional de atraso: {taxa_nacional:.2f}%")
print(
    f"Participação de referência: "
    f"{participacao_referencia:.2f}%"
)

display(
    df_matriz_logistica
    .select(
        "customer_state",
        "total_pedidos",
        "pedidos_atrasados",
        "taxa_atraso",
        "participacao_atrasos",
        "classificacao"
    )
    .orderBy(F.desc("pedidos_atrasados"))
)

# COMMAND ----------

# ==========================================================
# ETAPA 11.18 - CLASSIFICAÇÃO DOS ESTADOS
# ==========================================================

display(
    df_matriz_logistica
    .filter(
        F.col("customer_state").isin(
            "SP", "RJ", "MG", "AL"
        )
    )
    .select(
        "customer_state",
        "taxa_atraso",
        "participacao_atrasos",
        "classificacao"
    )
    .orderBy("customer_state")
)


# COMMAND ----------

# ==========================================================
# ETAPA 11.19 - RESUMO DA MATRIZ LOGÍSTICA
# ==========================================================

df_resumo_matriz = (
    df_matriz_logistica
    .groupBy("classificacao")
    .agg(
        F.count("*").alias("quantidade_estados"),

        F.sum("total_pedidos").alias("total_pedidos"),

        F.sum("pedidos_atrasados").alias("total_atrasos")
    )
    .withColumn(
        "participacao_atrasos",
        F.round(
            F.col("total_atrasos") /
            F.lit(total_atrasos) * 100,
            2
        )
    )
    .orderBy(F.desc("total_atrasos"))
)

display(df_resumo_matriz)

# COMMAND ----------

# ==========================================================
# ETAPA 11.20 - ESTADOS DE ALTA TAXA E ALTO IMPACTO
# ==========================================================

df_estados_prioritarios = (
    df_matriz_logistica
    .filter(
        F.col("classificacao") ==
        "Alta taxa / Alto impacto"
    )
    .select(
        "customer_state",
        "total_pedidos",
        "pedidos_atrasados",
        "taxa_atraso",
        "participacao_atrasos"
    )
    .orderBy(F.desc("pedidos_atrasados"))
)

display(df_estados_prioritarios)

# COMMAND ----------

# ==========================================================
# ETAPA 11.21 - LOGÍSTICA E SATISFAÇÃO DOS ESTADOS PRIORITÁRIOS
# ==========================================================

df_prioritarios_satisfacao = (
    df_estados_prioritarios
    .join(
        df_satisfacao_estado.select(
            "customer_state",
            "pedidos_avaliados",
            "nota_media",
            "percentual_negativas"
        ),
        on="customer_state",
        how="left"
    )
    .select(
        "customer_state",
        "total_pedidos",
        "pedidos_atrasados",
        "taxa_atraso",
        "participacao_atrasos",
        "pedidos_avaliados",
        "nota_media",
        "percentual_negativas"
    )
    .orderBy(
        F.desc("pedidos_atrasados")
    )
)

display(df_prioritarios_satisfacao)

# COMMAND ----------

# ==========================================================
# ETAPA 11.22 - AVALIAÇÕES NEGATIVAS POR ESTADO
# ==========================================================

display(
    df_prioritarios_satisfacao
    .select(
        "customer_state",
        "nota_media",
        "percentual_negativas"
    )
    .orderBy(
        F.desc("percentual_negativas")
    )
)

# COMMAND ----------

# ==========================================================
# ETAPA 11.23 - TABELA FINAL DE INDICADORES POR ESTADO
# ==========================================================

df_kpis_logistica_estado = (
    df_matriz_logistica
    .join(
        df_satisfacao_estado.select(
            "customer_state",
            "pedidos_avaliados",
            "nota_media",
            "percentual_negativas"
        ),
        on="customer_state",
        how="left"
    )
    .select(
        "customer_state",
        "total_pedidos",
        "pedidos_atrasados",
        "taxa_atraso",
        "participacao_atrasos",
        "pedidos_avaliados",
        "nota_media",
        "percentual_negativas",
        "classificacao"
    )
)

print("=== VALIDAÇÃO DA TABELA FINAL ===")

print(
    "Estados:",
    df_kpis_logistica_estado.count()
)

print(
    "Estados distintos:",
    df_kpis_logistica_estado
    .select("customer_state")
    .distinct()
    .count()
)

print(
    "Total de pedidos:",
    df_kpis_logistica_estado
    .agg(F.sum("total_pedidos"))
    .first()[0]
)

print(
    "Total de atrasos:",
    df_kpis_logistica_estado
    .agg(F.sum("pedidos_atrasados"))
    .first()[0]
)

# COMMAND ----------

# ==========================================================
# ETAPA 12.1 - VALIDAR GRANULARIDADE DOS PEDIDOS
# ==========================================================

print("=== TABELA DE PEDIDOS ===")

print(
    "Total de linhas:",
    df_pedidos.count()
)

print(
    "Pedidos distintos:",
    df_pedidos
    .select("order_id")
    .distinct()
    .count()
)

print(
    "Pedidos com ID nulo:",
    df_pedidos
    .filter(F.col("order_id").isNull())
    .count()
)

# COMMAND ----------

# ==========================================================
# ETAPA 12.2 - VALIDAR GRANULARIDADE DOS ITENS
# ==========================================================

print("=== TABELA DE ITENS DOS PEDIDOS ===")

print(
    "Total de linhas:",
    df_itens.count()
)

print(
    "Pedidos distintos:",
    df_itens
    .select("order_id")
    .distinct()
    .count()
)

print(
    "Itens distintos (pedido + item):",
    df_itens
    .select("order_id", "order_item_id")
    .distinct()
    .count()
)

print(
    "IDs de pedidos nulos:",
    df_itens
    .filter(F.col("order_id").isNull())
    .count()
)

# COMMAND ----------

# ==========================================================
# ETAPA 12.3 - PEDIDOS SEM ITENS REGISTRADOS
# ==========================================================

df_pedidos_sem_itens = (
    df_pedidos
    .join(
        df_itens.select("order_id").distinct(),
        on="order_id",
        how="left_anti"
    )
)

print("=== PEDIDOS SEM ITENS ===")

print(
    "Total de pedidos sem itens:",
    df_pedidos_sem_itens.count()
)

display(
    df_pedidos_sem_itens
    .groupBy("order_status")
    .count()
    .orderBy(F.desc("count"))
)

# COMMAND ----------

# ==========================================================
# ETAPA 12.4 - VALIDAR VALORES FINANCEIROS DOS ITENS
# ==========================================================

df_itens.select(
    F.count("*").alias("total_itens"),

    F.sum(
        F.when(F.col("price").isNull(), 1).otherwise(0)
    ).alias("precos_nulos"),

    F.sum(
        F.when(F.col("price") < 0, 1).otherwise(0)
    ).alias("precos_negativos"),

    F.sum(
        F.when(F.col("freight_value").isNull(), 1).otherwise(0)
    ).alias("fretes_nulos"),

    F.sum(
        F.when(F.col("freight_value") < 0, 1).otherwise(0)
    ).alias("fretes_negativos"),

    F.round(
        F.sum("price"), 2
    ).alias("valor_total_produtos"),

    F.round(
        F.sum("freight_value"), 2
    ).alias("valor_total_fretes")
).show(truncate=False)

# COMMAND ----------

# ==========================================================
# ETAPA 12.5 - CRIAR A TABELA FATO DE ITENS
# ==========================================================

fato_itens_vendas = (
    df_itens
    .select(
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "shipping_limit_date",
        F.col("price").alias("valor_produto"),
        F.col("freight_value").alias("valor_frete")
    )
    .withColumn(
        "valor_total_item",
        F.round(
            F.col("valor_produto") +
            F.col("valor_frete"),
            2
        )
    )
)

display(fato_itens_vendas.limit(10))

# COMMAND ----------

# ==========================================================
# ETAPA 12.6 - VALIDAR A TABELA FATO DE VENDAS
# ==========================================================

print("=== VALIDAÇÃO DA FATO DE VENDAS ===")

fato_itens_vendas.agg(
    F.count("*").alias("total_itens"),

    F.countDistinct(
        "order_id", "order_item_id"
    ).alias("itens_distintos"),

    F.countDistinct(
        "order_id"
    ).alias("pedidos_distintos"),

    F.round(
        F.sum("valor_produto"), 2
    ).alias("total_produtos"),

    F.round(
        F.sum("valor_frete"), 2
    ).alias("total_fretes"),

    F.round(
        F.sum("valor_total_item"), 2
    ).alias("total_geral")
).show(truncate=False)

# COMMAND ----------

# ==========================================================
# ETAPA 12.7 - VALIDAR A DIMENSÃO DE PRODUTOS
# ==========================================================

print("=== VALIDAÇÃO DOS PRODUTOS ===")

print(
    "Total de linhas:",
    df_produtos.count()
)

print(
    "Produtos distintos:",
    df_produtos
    .select("product_id")
    .distinct()
    .count()
)

print(
    "IDs de produtos nulos:",
    df_produtos
    .filter(F.col("product_id").isNull())
    .count()
)

print(
    "IDs de produtos duplicados:",
    df_produtos
    .groupBy("product_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

# COMMAND ----------

# ==========================================================
# ETAPA 12.8 - CRIAR DIMENSÃO DE PRODUTOS
# ==========================================================

dim_produtos = (
    df_produtos
    .select(
        "product_id",
        "product_category_name",
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm"
    )
)

print("=== DIMENSÃO DE PRODUTOS ===")

print("Total de produtos:", dim_produtos.count())

display(dim_produtos.limit(10))

# COMMAND ----------

# ==========================================================
# ETAPA 12.9 - INTEGRIDADE REFERENCIAL DOS PRODUTOS
# ==========================================================

df_produtos_orfaos = (
    fato_itens_vendas
    .join(
        dim_produtos.select("product_id"),
        on="product_id",
        how="left_anti"
    )
)

print("=== INTEGRIDADE REFERENCIAL ===")

print(
    "Itens sem produto correspondente:",
    df_produtos_orfaos.count()
)

print(
    "IDs de produtos nulos na fato:",
    fato_itens_vendas
    .filter(F.col("product_id").isNull())
    .count()
)

print(
    "Produtos distintos utilizados nas vendas:",
    fato_itens_vendas
    .select("product_id")
    .distinct()
    .count()
)

# COMMAND ----------

# Verificar os DataFrames disponíveis na sessão

variaveis = [
    nome for nome, valor in globals().items()
    if nome.startswith("df_")
]

print("DataFrames encontrados:")
for nome in sorted(variaveis):
    print(nome)

# COMMAND ----------

# ==========================================================
# ETAPA 12.10 - LOCALIZAR TABELA DE VENDEDORES
# ==========================================================

print("=== TABELAS DISPONÍVEIS NO DATABRICKS ===")

tabelas = spark.sql("SHOW TABLES")

display(
    tabelas.filter(
        F.lower(F.col("tableName")).contains("seller") |
        F.lower(F.col("tableName")).contains("vendedor")
    )
)

# COMMAND ----------

# ==========================================================
# ETAPA 12.10 - IDENTIFICAR A ORIGEM DOS DADOS
# ==========================================================

print("=== ORIGEM DO DATAFRAME DE PRODUTOS ===")

df_produtos.explain(mode="formatted")

# COMMAND ----------

# ==========================================================
# ETAPA 12.10 - LOCALIZAR ARQUIVO DE VENDEDORES
# ==========================================================

caminho_olist = "/Volumes/workspace/default/projeto_olist/"

arquivos = dbutils.fs.ls(caminho_olist)

print("=== ARQUIVOS DO PROJETO OLIST ===")

for arquivo in arquivos:
    print(arquivo.name)

# COMMAND ----------

# ==========================================================
# ETAPA 12.10 - CARREGAR E VALIDAR VENDEDORES
# ==========================================================

df_vendedores = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(
        "/Volumes/workspace/default/projeto_olist/olist_sellers_dataset.csv"
    )
)

print("=== VALIDAÇÃO DOS VENDEDORES ===")

print(
    "Total de vendedores:",
    df_vendedores.count()
)

print(
    "Vendedores distintos:",
    df_vendedores.select("seller_id").distinct().count()
)

print(
    "IDs nulos:",
    df_vendedores.filter(
        F.col("seller_id").isNull()
    ).count()
)

print(
    "IDs duplicados:",
    df_vendedores.groupBy("seller_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print("\n=== ESTRUTURA DA TABELA ===")
df_vendedores.printSchema()

# COMMAND ----------

# ==========================================================
# ETAPA 12.11 - CRIAR DIMENSÃO DE VENDEDORES
# ==========================================================

dim_vendedores = (
    df_vendedores
    .select(
        "seller_id",
        "seller_zip_code_prefix",
        "seller_city",
        "seller_state"
    )
)

print("=== DIMENSÃO DE VENDEDORES ===")

print(
    "Total de vendedores:",
    dim_vendedores.count()
)

print(
    "Quantidade de colunas:",
    len(dim_vendedores.columns)
)

display(dim_vendedores.limit(10))

# COMMAND ----------

# ==========================================================
# ETAPA 12.12 - VALIDAR RELACIONAMENTO DOS VENDEDORES
# ==========================================================

print("=== INTEGRIDADE REFERENCIAL DOS VENDEDORES ===")

vendedores_orfaos = (
    fato_itens_vendas
    .join(
        dim_vendedores.select("seller_id"),
        on="seller_id",
        how="left_anti"
    )
)

print(
    "Itens sem vendedor correspondente:",
    vendedores_orfaos.count()
)

print(
    "IDs de vendedores nulos na fato:",
    fato_itens_vendas
    .filter(F.col("seller_id").isNull())
    .count()
)

print(
    "Vendedores distintos nas vendas:",
    fato_itens_vendas
    .select("seller_id")
    .distinct()
    .count()
)

# COMMAND ----------

# ==========================================================
# ETAPA 12.13 - VALIDAR DADOS DA DIMENSÃO DE PEDIDOS
# ==========================================================

print("=== VALIDAÇÃO DOS PEDIDOS ===")

print(
    "Total de pedidos:",
    df_pedidos.count()
)

print(
    "Pedidos distintos:",
    df_pedidos.select("order_id").distinct().count()
)

print(
    "IDs de pedidos nulos:",
    df_pedidos.filter(
        F.col("order_id").isNull()
    ).count()
)

print(
    "Pedidos sem correspondência na fato:",
    df_pedidos.join(
        fato_itens_vendas.select("order_id").distinct(),
        on="order_id",
        how="left_anti"
    ).count()
)

print("\n=== COLUNAS DISPONÍVEIS ===")
df_pedidos.printSchema()

# COMMAND ----------

# ==========================================================
# ETAPA 12.14 - CRIAR DIMENSÃO DE PEDIDOS
# ==========================================================

dim_pedidos = (
    df_pedidos
    .select(
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    )
    .withColumn(
        "data_compra",
        F.to_date("order_purchase_timestamp")
    )
)

print("=== DIMENSÃO DE PEDIDOS ===")

print(
    "Total de pedidos:",
    dim_pedidos.count()
)

print(
    "Quantidade de colunas:",
    len(dim_pedidos.columns)
)

display(dim_pedidos.limit(10))

# COMMAND ----------

# ==========================================================
# ETAPA 12.15 - VALIDAR RELACIONAMENTO PEDIDOS E VENDAS
# ==========================================================

print("=== INTEGRIDADE REFERENCIAL DOS PEDIDOS ===")

# Itens sem pedido correspondente
itens_orfaos = (
    fato_itens_vendas
    .join(
        dim_pedidos.select("order_id"),
        on="order_id",
        how="left_anti"
    )
)

print(
    "Itens sem pedido correspondente:",
    itens_orfaos.count()
)

# IDs nulos na tabela fato
print(
    "IDs de pedidos nulos na fato:",
    fato_itens_vendas
    .filter(F.col("order_id").isNull())
    .count()
)

# Pedidos sem itens
pedidos_sem_itens = (
    dim_pedidos
    .join(
        fato_itens_vendas.select("order_id").distinct(),
        on="order_id",
        how="left_anti"
    )
)

print(
    "Pedidos sem itens:",
    pedidos_sem_itens.count()
)

# COMMAND ----------

# ==========================================================
# ETAPA 12.16 - VALIDAR A TABELA DE CLIENTES
# ==========================================================

print("=== VALIDAÇÃO DOS CLIENTES ===")

print(
    "Total de linhas:",
    df_clientes.count()
)

print(
    "customer_id distintos:",
    df_clientes
    .select("customer_id")
    .distinct()
    .count()
)

print(
    "customer_id nulos:",
    df_clientes
    .filter(F.col("customer_id").isNull())
    .count()
)

print(
    "customer_id duplicados:",
    df_clientes
    .groupBy("customer_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(
    "Consumidores únicos:",
    df_clientes
    .select("customer_unique_id")
    .distinct()
    .count()
)

print("\n=== ESTRUTURA DOS CLIENTES ===")
df_clientes.printSchema()

# COMMAND ----------

# ==========================================================
# ETAPA 12.17 - CRIAR DIMENSÃO DE CLIENTES
# ==========================================================

dim_clientes = (
    df_clientes
    .select(
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state"
    )
)

print("=== DIMENSÃO DE CLIENTES ===")

print(
    "Total de registros:",
    dim_clientes.count()
)

print(
    "Quantidade de colunas:",
    len(dim_clientes.columns)
)

display(dim_clientes.limit(10))

# COMMAND ----------

# ==========================================================
# ETAPA 12.18 - VALIDAR RELACIONAMENTO CLIENTES E PEDIDOS
# ==========================================================

print("=== INTEGRIDADE REFERENCIAL DOS CLIENTES ===")

# Pedidos sem cliente correspondente
pedidos_sem_cliente = (
    dim_pedidos
    .join(
        dim_clientes.select("customer_id"),
        on="customer_id",
        how="left_anti"
    )
)

print(
    "Pedidos sem cliente correspondente:",
    pedidos_sem_cliente.count()
)

# IDs de clientes nulos nos pedidos
print(
    "customer_id nulos nos pedidos:",
    dim_pedidos
    .filter(F.col("customer_id").isNull())
    .count()
)

# IDs de consumidores únicos nulos
print(
    "customer_unique_id nulos:",
    dim_clientes
    .filter(F.col("customer_unique_id").isNull())
    .count()
)

# Clientes associados a pedidos
print(
    "Clientes distintos nos pedidos:",
    dim_pedidos
    .select("customer_id")
    .distinct()
    .count()
)

# COMMAND ----------

# ==========================================================
# ETAPA 12.19 - CRIAR DIMENSÃO CALENDÁRIO
# ==========================================================

# Identificar o intervalo das compras
periodo = dim_pedidos.agg(
    F.min("data_compra").alias("data_inicial"),
    F.max("data_compra").alias("data_final")
).first()

data_inicial = periodo["data_inicial"]
data_final = periodo["data_final"]

# Gerar uma linha para cada dia do período
dim_calendario = (
    spark.sql(
        f"""
        SELECT explode(
            sequence(
                to_date('{data_inicial}'),
                to_date('{data_final}'),
                interval 1 day
            )
        ) AS data
        """
    )
    .withColumn("ano", F.year("data"))
    .withColumn("mes", F.month("data"))
    .withColumn("trimestre", F.quarter("data"))
    .withColumn("ano_mes", F.date_format("data", "yyyy-MM"))
    .withColumn("dia", F.dayofmonth("data"))
    .withColumn("dia_semana", F.dayofweek("data"))
)

print("=== DIMENSÃO CALENDÁRIO ===")

print("Data inicial:", data_inicial)
print("Data final:", data_final)
print("Total de dias:", dim_calendario.count())
print("Quantidade de colunas:", len(dim_calendario.columns))

display(dim_calendario.limit(10))

# COMMAND ----------

# ==========================================================
# ETAPA 12.20 - VERIFICAR CATÁLOGO E ESQUEMA
# ==========================================================

print("=== AMBIENTE DO DATABRICKS ===")

spark.sql("""
    SELECT
        current_catalog() AS catalogo,
        current_schema() AS esquema
""").show(truncate=False)

print("=== TABELAS EXISTENTES ===")

spark.sql("SHOW TABLES").show(
    50,
    truncate=False
)

# COMMAND ----------

# ==========================================================
# ETAPA 12.21 - SALVAR MODELO DIMENSIONAL EM DELTA
# ==========================================================

tabelas_modelo = {
    "fato_itens_vendas": fato_itens_vendas,
    "dim_produtos": dim_produtos,
    "dim_vendedores": dim_vendedores,
    "dim_pedidos": dim_pedidos,
    "dim_clientes": dim_clientes,
    "dim_calendario": dim_calendario
}

for nome, dataframe in tabelas_modelo.items():

    tabela_destino = f"workspace.default.{nome}"

    (
        dataframe.write
        .format("delta")
        .mode("errorifexists")
        .saveAsTable(tabela_destino)
    )

    print(f"Tabela criada: {tabela_destino}")

print("\n=== TABELAS DO MODELO SALVAS ===")

# COMMAND ----------

# ==========================================================
# ETAPA 12.21 - SALVAR TABELAS DELTA COM SEGURANÇA
# ==========================================================

tabelas_modelo = {
    "fato_itens_vendas": fato_itens_vendas,
    "dim_produtos": dim_produtos,
    "dim_vendedores": dim_vendedores,
    "dim_pedidos": dim_pedidos,
    "dim_clientes": dim_clientes,
    "dim_calendario": dim_calendario
}

print("=== SALVANDO TABELAS DO MODELO ===")

for nome, dataframe in tabelas_modelo.items():

    tabela_destino = f"workspace.default.{nome}"

    if spark.catalog.tableExists(tabela_destino):
        print(f"JÁ EXISTE (não alterada): {tabela_destino}")

    else:
        (
            dataframe.write
            .format("delta")
            .mode("error")
            .saveAsTable(tabela_destino)
        )

        print(f"CRIADA: {tabela_destino}")

print("\n=== PROCESSO FINALIZADO ===")

# COMMAND ----------

# ==========================================================
# ETAPA 12.22 - VALIDAR TABELAS DELTA SALVAS
# ==========================================================

tabelas_esperadas = {
    "fato_itens_vendas": 112650,
    "dim_produtos": 32951,
    "dim_vendedores": 3095,
    "dim_pedidos": 99441,
    "dim_clientes": 99441,
    "dim_calendario": 774
}

print("=== VALIDAÇÃO DAS TABELAS DELTA ===")

for nome, esperado in tabelas_esperadas.items():

    tabela = f"workspace.default.{nome}"

    encontrado = spark.table(tabela).count()

    situacao = (
        "OK" if encontrado == esperado
        else "DIVERGÊNCIA"
    )

    print(
        f"{nome}: "
        f"{encontrado} registros | "
        f"Esperado: {esperado} | "
        f"{situacao}"
    )

# COMMAND ----------

# ==========================================================
# ETAPA 13.2 - EXPORTAR DIMENSÃO CALENDÁRIO
# ==========================================================

caminho_exportacao = (
    "/Volumes/workspace/default/projeto_olist/"
    "export_powerbi/dim_calendario"
)

(
    spark.table("workspace.default.dim_calendario")
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", True)
    .option("dateFormat", "yyyy-MM-dd")
    .csv(caminho_exportacao)
)

print("Exportação concluída!")

display(dbutils.fs.ls(caminho_exportacao))

# COMMAND ----------

# ==========================================================
# ETAPA 13.3 - PREPARAR CSV PARA DOWNLOAD
# ==========================================================

pasta = (
    "dbfs:/Volumes/workspace/default/projeto_olist/"
    "export_powerbi/dim_calendario/"
)

arquivos = dbutils.fs.ls(pasta)

arquivo_csv = [
    arquivo.path
    for arquivo in arquivos
    if arquivo.name.startswith("part-")
    and arquivo.name.endswith(".csv")
][0]

destino = (
    "dbfs:/Volumes/workspace/default/projeto_olist/"
    "export_powerbi/dim_calendario.csv"
)

dbutils.fs.cp(arquivo_csv, destino)

print("Arquivo preparado com sucesso!")
print("Destino:", destino)

# COMMAND ----------

# ==========================================================
# ETAPA 13.7 - EXPORTAR TABELAS PARA O POWER BI
# ==========================================================

pasta_base = (
    "dbfs:/Volumes/workspace/default/projeto_olist/"
    "export_powerbi/"
)

nomes_tabelas = [
    "fato_itens_vendas",
    "dim_produtos",
    "dim_vendedores",
    "dim_pedidos",
    "dim_clientes"
]

print("=== EXPORTAÇÃO PARA POWER BI ===")

for nome in nomes_tabelas:

    origem = f"workspace.default.{nome}"
    pasta_temporaria = f"{pasta_base}{nome}/"
    arquivo_final = f"{pasta_base}{nome}.csv"

    # Ler tabela Delta e exportar como CSV
    (
        spark.table(origem)
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .csv(pasta_temporaria)
    )

    # Localizar o CSV gerado pelo Spark
    arquivos = dbutils.fs.ls(pasta_temporaria)

    partes_csv = [
        arquivo.path
        for arquivo in arquivos
        if arquivo.name.startswith("part-")
        and arquivo.name.endswith(".csv")
    ]

    if len(partes_csv) != 1:
        raise RuntimeError(
            f"Esperado 1 CSV para {nome}, "
            f"encontrados {len(partes_csv)}"
        )

    # Criar arquivo com nome padronizado
    dbutils.fs.cp(
        partes_csv[0],
        arquivo_final
    )

    print(f"EXPORTADO: {nome}.csv")

print("\n=== EXPORTAÇÃO FINALIZADA ===")