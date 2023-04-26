-- Databricks notebook source


-- COMMAND ----------

WITH tb_pedidos AS (
  SELECT 
    DISTINCT
    t1.idPedido,
    t2.idVendedor

FROM silver.olist.pedido AS t1

LEFT JOIN silver.olist.item_pedido AS t2
ON t1.idPedido = t2.idPedido

WHERE t1.dtPedido < '2018-01-01'
AND t1.dtPedido >= add_months('2018-01-01', -6)
AND t2.idVendedor IS NOT NULL
),


tb_join AS (
  SELECT t1.idVendedor ,
         t2.* 
         
  FROM tb_pedidos AS t1

  LEFT JOIN silver.olist.pagamento_pedido AS t2
  ON t1.idPedido = t2.idPedido
),

tb_group AS (
  SELECT idVendedor,
       descTipoPagamento,
       count(distinct idPedido) AS qtdePedidoMeioPagamento,
       sum(vlPagamento) as vlPedidoMeioPagamento

  FROM tb_join

  GROUP BY idVendedor, descTipoPagamento
),


tb_summary AS (
SELECT idVendedor,

sum(case when descTipoPagamento = 'boleto' then qtdePedidoMeioPagamento else 0 end) as qtde_boleto_pedido,
sum(case when descTipoPagamento = 'credit_card' then qtdePedidoMeioPagamento else 0 end) as qtde_credit_pedido,
sum(case when descTipoPagamento = 'voucher' then qtdePedidoMeioPagamento else 0 end) as qtde_voucher_pedido ,
sum(case when descTipoPagamento = 'debit_card' then qtdePedidoMeioPagamento else 0 end) as qtde_debit_card_pedido,

sum(case when descTipoPagamento = 'boleto' then vlPedidoMeioPagamento else 0 end) as valor_boleto_pedido,
sum(case when descTipoPagamento = 'credit_card' then vlPedidoMeioPagamento else 0 end) as valor_credit_pedido,
sum(case when descTipoPagamento = 'voucher' then vlPedidoMeioPagamento else 0 end) as valor_voucher_pedido ,
sum(case when descTipoPagamento = 'debit_card' then vlPedidoMeioPagamento else 0 end) as valor_debit_card_pedido,

sum(case when descTipoPagamento = 'boleto' then qtdePedidoMeioPagamento else 0 end)/sum(qtdePedidoMeioPagamento) as pct_boleto_pedido,
sum(case when descTipoPagamento = 'credit_card' then qtdePedidoMeioPagamento else 0 end)/sum(qtdePedidoMeioPagamento) as pct_credit_pedido,
sum(case when descTipoPagamento = 'voucher' then qtdePedidoMeioPagamento else 0 end)/sum(qtdePedidoMeioPagamento) as pct_voucher_pedido ,
sum(case when descTipoPagamento = 'debit_card' then qtdePedidoMeioPagamento else 0 end)/sum(qtdePedidoMeioPagamento) as pct_debit_card_pedido,

sum(case when descTipoPagamento = 'boleto' then vlPedidoMeioPagamento else 0 end)/sum(vlPedidoMeioPagamento) as pct_valor_boleto_pedido,
sum(case when descTipoPagamento = 'credit_card' then vlPedidoMeioPagamento else 0 end)/sum(vlPedidoMeioPagamento) as pct_valor_credit_pedido,
sum(case when descTipoPagamento = 'voucher' then vlPedidoMeioPagamento else 0 end)/sum(vlPedidoMeioPagamento) as pct_valor_voucher_pedido ,
sum(case when descTipoPagamento = 'debit_card' then vlPedidoMeioPagamento else 0 end)/sum(vlPedidoMeioPagamento) as pct_valor_debit_card_pedido

FROM tb_group

GROUP BY idVendedor
),



tb_cartao AS (
SELECT idVendedor,
       AVG(nrParcelas) AS avgQtdeParcelas,
       percentile(nrParcelas, 0.5) AS medianQtdeParcelas,
       MIN(nrParcelas) AS minQtdeParcelas,
       MAX(nrParcelas) AS maxQtdeParcelas

FROM tb_join 

WHERE descTipoPagamento = 'credit_card'

GROUP BY idVendedor
)


SELECT 
      '2018-01-01' as dtReference,
       t1.*,
       t2.avgQtdeParcelas,
       t2.medianQtdeParcelas,
       t2.minQtdeParcelas,
       t2.maxQtdeParcelas


FROM tb_summary as t1

LEFT JOIN tb_cartao as t2 

ON t1.idVendedor = t2.idVendedor



-- COMMAND ----------

SELECT 
  DISTINCT
  t1.idPedido,
  t2.idVendedor

FROM silver.olist.pedido AS t1

LEFT JOIN silver.olist.item_pedido AS t2
ON t1.idPedido = t2.idPedido

WHERE t1.dtPedido < '2018-01-01'
AND t1.dtPedido >= add_months('2018-01-01', -6)


-- COMMAND ----------


