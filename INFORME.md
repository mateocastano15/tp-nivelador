# Informe

## Protocolo de comunicación

Formato TLV (Type-Length-Value) big-endian.

```
[TYPE: 2 bytes][LENGTH: 2 bytes][VALUE: LENGTH bytes]
```

### Tipos de mensaje

| Tipo | Valor | Value |
|---|---|---|
| BET | 01 | Compuesto |
| END_OF_BETS | 02 | agency_id |
| ACK | 03 | Vacío |
| BATCH | 04 | Concatenación de N bets de tipo BET (01) |

### Estructura de un BET

El `value` de un BET es a su vez una concatenación de campos, cada uno con su propio header type+length:

| Campo | Tipo | Contenido |
|---|---|---|
| AGENCY_ID | 01 | decimal como texto |
| FIRST_NAME | 02 | texto |
| LAST_NAME | 03 | texto |
| DOCUMENT | 04 | decimal como texto |
| BIRTHDATE | 05 | texto (YYYY-MM-DD) |
| NUMBER | 06 | decimal como texto |

Todos los valores viajan como texto (bytes de su representación en string).

### Estructura de un BATCH

El `value` de un BATCH es la concatenación de N apuestas, cada una ya envuelta en su propio header `[TYPE=01][LENGTH][value]`:

```
[TYPE=04][LENGTH][ [01][len_1][bet_1] [01][len_2][bet_2] ... [01][len_N][bet_N] ]
```

### Flujo de mensajes

1. Cliente -> Servidor: uno o más `BATCH`, cada uno con hasta `BATCH_SIZE` apuestas. El último batch de la ejecución puede tener menos, si la cantidad total de apuestas no es múltiplo de `BATCH_SIZE`.
2. Servidor -> Cliente: un `ACK` por cada `BATCH` recibido y almacenado.
3. Cliente -> Servidor: al agotar el archivo de entrada, un `END_OF_BETS` con el `agency_id`.
4. Servidor -> Cliente: un `BATCH` con las apuestas ganadoras de esa agencia (puede ir vacío).

Implementación: [protocol.py](services/server/src/protocol/protocol.py) / [protocol.go](services/client/src/client/protocol.go).

## Concurrencia y sincronización

El servidor acepta conexiones concurrentemente: un thread dedicado corre `_accept_connections`, y por cada conexión aceptada se lanza un thread nuevo (`_handle_client`) que atiende a esa agencia de punta a punta. No se hace broadcast de los ganadores: cada thread calcula y responde solo los ganadores de su propia agencia, filtrando por `agency_id`.

### Atributos de `Server`

| Atributo | Tipo | Rol |
|---|---|---|
| `server_host`, `server_port` | str, int | dirección donde escucha el socket de aceptación |
| `agency_quorum_min` | int | cantidad mínima de agencias que deben terminar de enviar apuestas antes de sortear |
| `lottery` | `Lottery` | persistencia/lectura de `bets.csv` y cálculo de ganadores (provisto, no se modifica) |
| `finished_agencies` | int | contador de agencias que ya terminaron de enviar sus apuestas |
| `lock` | `threading.Lock` | protege el acceso concurrente a `finished_agencies` y a `lottery` |
| `quorum_reached` | `threading.Event` | señaliza que se alcanzó el quorum |
| `shutdown_event` | `threading.Event` | señaliza que se recibió SIGTERM |
| `client_handlers` | list de `(Thread, socket)` | threads y sockets de cada agencia conectada, para poder desbloquearlos y esperarlos al hacer shutdown |

Cada thread de agencia, bajo `lock`, guarda sus apuestas y aumenta `finished_agencies`; si con eso se alcanza `agency_quorum_min`, dispara `quorum_reached`. Después de soltar el lock, espera en `quorum_reached.wait()` — así ningún thread calcula ganadores hasta que la última agencia haya terminado de persistir las suyas.

## Graceful shutdown (SIGTERM)

### Servidor

`run()` bloquea SIGTERM con `pthread_sigmask(SIG_BLOCK, {SIGTERM})` antes de lanzar cualquier thread. Como la máscara de señales se hereda, ningún thread hijo (`_accept_connections`, `_handle_client`) puede terminar manejando la señal por su cuenta: queda centralizada en un único punto, un `signal.sigwait({SIGTERM})` bloqueante en el thread principal.

Al llegar la señal:

1. Se marca `shutdown_event` y se dispara `quorum_reached` (por si había threads de agencia esperando el quorum que nunca iba a llegar).
2. Se hace `shutdown(SHUT_RDWR)` sobre el socket de escucha, para desbloquear el `accept()` bloqueado en el thread de `_accept_connections`, y se lo joinea.
3. Se hace `shutdown(SHUT_RDWR)` sobre cada socket de cliente ya aceptado (`client_handlers`), para desbloquear cualquier `recv()` en curso en esos threads, y se los joinea a todos.

Tanto el thread de `_accept_connections` como los de `_handle_client` se joinean antes de que `run()` retorne, así que todos los recursos quedan cerrados antes de que el hilo principal finalice.

Un thread de agencia que estaba esperando en `quorum_reached.wait()` cuando llega la señal, al despertarse, chequea `shutdown_event` y corta sin calcular ganadores. Uno que estaba bloqueado en `recv()` se entera por el `shutdown()` del paso 3, su lectura falla, y termina por la rama de excepción. En ambos casos el socket del cliente se cierra igual, porque `_handle_client` envuelve todo su cuerpo en `with client_socket:`.

### Cliente

Un goroutine dedicado escucha SIGTERM (`signal.Notify`). Al recibirla, cierra un channel `done` y fuerza `conn.Close()`. Cerrar una conexión mientras otro goroutine la está usando (leyendo o escribiendo) es una operación segura y soportada en Go — a diferencia del caso de Python, acá no hace falta nada más para desbloquear un `Read`/`Write` en curso.

El loop principal de envío de apuestas chequea el channel `done` entre cada apuesta, para poder cortar en cualquier punto sin esperar a terminar el archivo de entrada. Todos los recursos (`conn`, archivo de entrada, archivo de salida) se cierran vía `defer`, así que se liberan pase lo que pase, incluso si se corta a mitad de camino.

Diferenciar un shutdown pedido de un error real: `shuttingDownOr(done, err)` chequea si `done` ya está cerrado antes de decidir qué devolver. Si es así, devuelve `ErrShuttingDown{}` en vez del error de conexión que causó el corte (por ejemplo, un `recv` fallido porque `conn.Close()` lo interrumpió). En `main.go`, `ErrShuttingDown` se interpreta como salida exitosa (exit code 0) — sin este chequeo, cualquier shutdown por señal se reportaría como un fallo real del proceso.
