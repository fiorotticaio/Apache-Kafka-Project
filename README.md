# Apache-Kafka-Project

## Topicos
	1. coffee_stock = - Pega o preço da ação do café a cada minuto;
										- Uma partição; 
										- Replicação de 1;
	2. coffee_price = - Armazena o preço do café gerado pela ação, o preço gerado pela 
											web page e o preço real (que aparece na web page);
										- 3 partições, um para cada preço;
											- partição 0 = preço do café pela ação;
											- partição 1 = preço do café gerado pela web page;
											- partição 2 = preço real do café;
										- Replicação de 1;

## Instructions

Remember to clear all kafka cache before initializing producers and consumers

## Useful links
API documentation
https://www.alphavantage.co/documentation/

Endpoint for stabucks in each minute
https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&interval=1min&symbol=SBUX&apikey=suappikey

To have the API key just give the email on the main page https://www.alphavantage.co/support/#api-key
And them you substitute the suappikey with your key

## Ideias da Patrícia
user partições diferentes
	- usar ações do starbucks de diferentes regiões
	- inferencia de eventos de cada partição sobre cada região

caso starbucks america esteja maior que a europa
	- gerar um evento
	
usar criatividade e verificar riqueza da api

objetivo nessse trab 1 é entender o kafka
	- conceitos
	- partições
	- leituras separadas

pode ser consumidores e produtores separados

pode ser qualquer linguagem no frontend