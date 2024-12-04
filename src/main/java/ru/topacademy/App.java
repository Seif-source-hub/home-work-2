package ru.topacademy;

import ru.topacademy.domain.Symbol;
import ru.topacademy.config.KafkaConfig;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App 
{
    private static final Logger logger = LoggerFactory.getLogger(App.class);
    private static final int MAX_MESSAGE = 5;
    private static final String[] ARRAY_VOWELS = {"a", "e", "i", "o", "u"};
    private static final String[] ARRAY_CONSONANT = {"b", "c", "d", "f", "g"};

    public static void main( String[] args )
    {
        System.out.println("Start...");
        try (KafkaProducer<Long, Symbol> producer = new KafkaProducer<>(KafkaConfig.getProducerConfig())) {
            for (int i = 0; i < MAX_MESSAGE; i++) {
                Symbol vowelSymbol = createSymbol(i, ARRAY_VOWELS[i], "red", "vowel");
                Symbol consonantSymbol = createSymbol(i, ARRAY_CONSONANT[i], "blue", "consonant");

                /**
                 * Конструктор ProducerRecord(topic, partition, timestamp, key, value) принимает в качестве аргументов:
                 * - topic - номер топика
                 * - partition - номер партиции           (опция)
                 * - timestamp - время создания сообщения (опция)
                 * - key - ключ id экземпляра Symbol      (опция)
                 * - value - объект Symbol
                 *
                 * Варианты конструкторов:
                 * - ProducerRecord(topic, value)
                 * - ProducerRecord(topic, key, value)
                 * - ProducerRecord(topic, partition, key, value)
                 * - ProducerRecord(topic, partition, key, value, headers)
                 */
                long timestamp = System.currentTimeMillis();
                ProducerRecord<Long, Symbol> producerRecordVowels = new ProducerRecord<>(KafkaConfig.VOWELS, KafkaConfig.PARTITION,
                        timestamp, vowelSymbol.getId(), vowelSymbol);
                ProducerRecord<Long, Symbol> producerRecordConsonants = new ProducerRecord<>(KafkaConfig.CONSONANTS, KafkaConfig.PARTITION,
                        timestamp, consonantSymbol.getId(), consonantSymbol);

                /**
                 * Анонимный внутренний класс (Callback), содержащий только один метод onCompletion(), можно записать
                 * через лямбду
                 */
                producer.send(producerRecordVowels, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            logger.error("Error sending message: {}", e.getMessage(), e);
                        } else {
                            logger.info("Sent record: key={}, value={}, partition={}, offset={}",
                                    vowelSymbol.getId(), vowelSymbol, recordMetadata.partition(), recordMetadata.offset());                        }
                    }
                });
                logger.info("Отправлено сообщение: key-{}, value-{}", i, vowelSymbol);
                producer.send(producerRecordConsonants, (recordMetadata, e) -> {
                    if (e != null) {
                        logger.error("Error sending message: {}", e.getMessage(), e);
                    } else {
                        logger.info("Sent record: key={}, value={}, partition={}, offset={}",
                                consonantSymbol.getId(), consonantSymbol, recordMetadata.partition(), recordMetadata.offset());
                    }
                });
                logger.info("Отправлено сообщение: key-{}, value-{}", i, consonantSymbol);
            }
            logger.info("Отправка завершена.");
        } catch (Exception e) {
            logger.error("Ошибка при отправке сообщений в Kafka", e);
        }
    }
    private static Symbol createSymbol(int index, String value, String color, String type) {
        return new Symbol(index, value, color, type);
    }
}
