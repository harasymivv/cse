import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class FoodDeliverySystem {
    public static void main(String[] args) throws Exception {
        // Створюємо середовище виконання для потокової обробки
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Властивості для підключення до Apache Kafka
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProps.setProperty("group.id", "food-delivery-group");

        // Створюємо потік даних з Apache Kafka
        DataStream<String> orders = env
                .addSource(new FlinkKafkaConsumer<>("food-orders", new SimpleStringSchema(), kafkaProps));

        // Обробляємо потік даних
        DataStream<Order> processedOrders = orders
                .flatMap(new OrderParser()); // Розбиваємо рядок на об'єкт Order

        // Обчислюємо вартість доставки
        DataStream<DeliveryFee> deliveryFees = processedOrders
                .flatMap(new DeliveryFeeCalculator()); // Розраховуємо вартість доставки

        // Записуємо оброблені замовлення та вартість доставки в Apache Kafka
        processedOrders.addSink(new FlinkKafkaProducer<>("processed-orders", new SimpleStringSchema(), kafkaProps));
        deliveryFees.addSink(new FlinkKafkaProducer<>("delivery-fees", new SimpleStringSchema(), kafkaProps));

        // Запускаємо виконання
        env.execute("Food Delivery System");
    }

       public static class Order {
        public String customerId;
        public String items;
        public String address;
        public double totalPrice;

        public Order() {}

        public Order(String customerId, String items, String address, double totalPrice) {
            this.customerId = customerId;
            this.items = items;
            this.address = address;
            this.totalPrice = totalPrice;
        }
    }

    // Клас для представлення вартості доставки
    public static class DeliveryFee {
        public String orderId;
        public double fee;

        public DeliveryFee() {}

        public DeliveryFee(String orderId, double fee) {
            this.orderId = orderId;
            this.fee = fee;
        }
    }

    // Функція для розбиття рядка на об'єкт Order
    private static class OrderParser implements FlatMapFunction<String, Order> {
        @Override
        public void flatMap(String value, Collector<Order> out) {
            // Очікуваний формат рядка: "customerId|items|address|totalPrice"
            String[] parts = value.split("\\|");
            if (parts.length == 4) {
                String customerId = parts[0];
                String items = parts[1];
                String address = parts[2];
                double totalPrice = Double.parseDouble(parts[3]);
                out.collect(new Order(customerId, items, address, totalPrice));
            }
        }
    }

    // Функція для обчислення вартості доставки
    private static class DeliveryFeeCalculator implements FlatMapFunction<Order, DeliveryFee> {
        @Override
        public void flatMap(Order order, Collector<DeliveryFee> out) {
            // Припустимо, що вартість доставки залежить від загальної вартості замовлення
            double deliveryFee = 0.0;
            if (order.totalPrice < 20.0) {
                deliveryFee = 3.0; // Фіксована вартість доставки для замовлень менше $20
            } else {
                deliveryFee = order.totalPrice * 0.1; // 10% від загальної вартості замовлення
            }
            out.collect(new DeliveryFee(order.customerId, deliveryFee));
        }
    }

}