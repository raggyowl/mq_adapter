MQ_ADAPTER
===========
 Run as Docker container:
 ----
```sh
$docker run -d walkohm/mq_adapter \
--name adapter --port 9090:9090 \
--env RABBIT_URL=amqp://guest:guest@rabbit.host:5672 \
--env OUT_URL=http://example.com:8080 \
--env INPUT_EXCHANGE=test_input_exchange \
--env OUTPUT_EXCHANGE=test_output_exchange
```
 Run in sh (you have installed [golang](https://golang.org/doc/install)):
 ----
1. Set environment variables
    ```sh
    $export RABBIT_URL=amqp://guest:guest@rabbit.host:5672
    $export OUT_URL=http://example.com:8080
    $export INPUT_EXCHANGE=test_input_exchange
    $export OUTPUT_EXCHANGE=test_output_exchange
    ```
2. Run
    ```sh
    $git clone https://github.com/walkohm/mq_adapter && cd mq_adapter
    $go build -o ./build/mq_adapter
    $sh ./build/mq_adapter
    ```
