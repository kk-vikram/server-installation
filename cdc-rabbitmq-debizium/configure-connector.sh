curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" http://localhost:8083/connectors/ -d '{
  "name": "inventory-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "postgres",
    "database.password": "postgres",
    "database.dbname": "inventory",
    "database.server.name": "dbserver1",
    "table.include.list": "public.customers",
    "plugin.name": "pgoutput",
    "slot.name": "inventory_slot",
    "publication.name": "inventory_publication",
    "topic.prefix": "dbserver1",
    "transforms": "route",
    "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
    "transforms.route.regex": "([^.]+)\\.([^.]+)\\.([^.]+)",
    "transforms.route.replacement": "$1.$2.$3",
    "sink.type": "amqp",
    "sink.amqp.url": "amqp://guest:guest@rabbitmq:5672",
    "sink.amqp.exchange": "",
    "sink.amqp.routing.key": "dbserver1.public.customers"
  }
}'
