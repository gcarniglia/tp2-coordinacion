# Trabajo Práctico - Coordinación

En este trabajo se busca familiarizar a los estudiantes con los desafíos de la coordinación del trabajo y el control de la complejidad en sistemas distribuidos. Para tal fin se provee un esqueleto de un sistema de control de stock de una verdulería y un conjunto de escenarios de creciente grado de complejidad y distribución que demandarán mayor sofisticación en la comunicación de las partes involucradas.

---

## Ejecución

- `make up`  
  Inicia los contenedores del sistema y comienza a seguir los logs de todos ellos en un solo flujo de salida.

- `make down`  
  Detiene los contenedores y libera los recursos asociados.

- `make logs`  
  Sigue los logs de todos los contenedores en un solo flujo de salida.

- `make test`  
  Inicia los contenedores del sistema, espera a que los clientes finalicen, compara los resultados con una ejecución serial y detiene los contenedores.

- `make switch`  
  Permite alternar rápidamente entre los archivos de docker compose de los distintos escenarios provistos.

---

## Mi solución

### Protocolo interno

Los mensajes siguen siempre una estructura uniforme:

{
  "type": number,
  "source_client_uuid": "string",
  "data": "Any (generalmente un array conteniendo datos)"
}

Para los tipos de mensajes se utiliza el siguiente formato:

{WorkerOrigen}\_{WorkerDestino}\_{Objetivo}

Por ejemplo:

- GAT_SUM_EOF: mensaje del gateway a los workers sum indicando EOF (end of data sent).

---

### Diseño

En lo que refiere al diseño de robustez, se mantuvo el diseño original. El único cambio es que se agregó una capa de sincronización entre cada instancia de los worker sum para cuando se envía un EOF de datos de un cliente.

![ ](./imgs/diagrama_de_robustez.jpg "Diagrama de Robustez")
*Fig. 1: Diagrama de Robustez Original*

---

### Flujo de mensajería (para un cliente, por simplificación)

- Se envía GAT_SUM_DATA desde el Gateway a la cola de trabajo de los sum.
- Todos los sum van leyendo cada mensaje con cada par (fruta, cantidad).
- En un determinado momento un worker sum recibirá un mensaje GAT_SUM_EOF indicando que finalizó el envío de datos de esa solicitud del cliente.
- Este mensaje inicia el proceso de sincronización de los worker sum (que explicare por separado).
- Se procederá eventualmente a enviar los datos a los aggregators con el mensaje SUM_AGG_DATA.
- Los workers aggregators recibirán dicho mensaje. Estos workers poseen una cola dedicada por cada instancia.
- Cuando el worker sum finaliza de enviar todos los datos del cliente enviará un SUM_AGG_EOF a la cola de cada aggregator.
- Cuando los aggregator reciban un SUM_AGG_EOF equivalente a la cantidad de SUMs existentes, entonces considerarán que finalizó la etapa de agregación y pasarán a enviar un top 3 parcial al join mediante AGG_JOIN_DATA, donde cada mensaje es un dato del top. Cuando finaliza envía AGG_JOIN_EOF.
- El worker join recibirá AGG_JOIN_DATA y finalizará al recibir AGG_JOIN_EOF, enviando el resultado final al gateway con un JOIN_GAT_DATA.

#### Consideraciones del flujo de mensajería

- Para asegurar de manera determinista que una fruta de un cliente específico vaya siempre al mismo aggregator, y para a su vez garantizar una distribución no privilegiada hacia ninguna instancia específica, se utilizó hashlib para generar un valor que permita particionar la relación cliente-fruta hacia un aggregator determinado. Inicialmente se utilizó hash() de Python, pero se descartó por no ser determinista (mismos datos iban a diferentes aggregators).

### Flujo de mensajería de sincronización para los workers sum (un cliente)

- Un worker sum recibe el mensaje GAT_SUM_EOF por la cola del Gateway. En dicho mensaje esta totalizada la cantidad de lineas procesadas para ese cliente (tambien se lo puede ver como cantidad de paquetes, es lo mismo en este caso)
- Ese worker envía un mensaje por un exchange de control SUM_EOF_REQ, con su cantidad de lineas procesadas y el total que informó el gateway
- Cuando los otros worker sum reciben SUM_EOF_REQ, envían por ese mismo exchange (esto es, broadcast a todos los otros worker sum) un mensaje SUM_EOF_REP con los datos de cantidad de lineas procesados por ellos mismos
- Al final de la comunicación mencionada, todos los workers procedaran a tener el total de lineas enviadas por el gateway y la cantidad de lineas procesadas por cada worker sum. En ese marco, cada instancia realiza por su cuenta el Consenso EOF: si el total informado por el gateway es igual a la suma de lo reportado por cada worker sum, entonces se considera finalizado el consenso y se procede a enviar SUM_AGG_DATA al aggregator. Caso contrario, se vuelve a solicitar SUM_EOF_REQ (con un exponential backoff)

#### Consideraciones del flujo de sincronización

Por como esta diseñado el proceso, da la impresion de haber redundancia de mensajes en el sentido de que hay broadcasting constante si ocurren reintentos. Esto esta contemplado así: el objetivo es que todos los sums esten de acuerdo antes de pasar a la siguiente etapa. La forma mas estricta de que esto pase es que todos sepan toda la informacion de procesamiento.

Si bien no esta contemplado para este TP la tolerancia a fallos, no se puede dejar los reintentos infinitamente. Es por ello que si se supera la cantidad de intentos, se propaga el error de consenso considerando al cliente como finalizado. Esto no es correcto per se, pero sentí que si consideraba ese escenario estaba excediendome del alcance de este TP

---

## Presuposiciones de la solución

- Un cliente envía una única petición. Se dejó de esa forma. No es complicada la modificación, pero la solución actual no requería
- Una vez que se confirma el proceso de EOF, dicha instancia pasará a limpiar los datos de ese cliente. No se guarda en memoria nada que no sea estrictamente aquello que se está procesando.
- Una mejora posible sería agregar persistencia en archivos para guardar los datos. No se implementó, pero se consideró.
