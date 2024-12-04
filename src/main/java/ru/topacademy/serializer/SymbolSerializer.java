package ru.topacademy.serializer;

import ru.topacademy.domain.Symbol;
import org.apache.kafka.common.serialization.Serializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;

public class SymbolSerializer implements Serializer<Symbol> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, Symbol data) {
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new RuntimeException("Error serializing Symbol to JSON", e);
        }
    }

    @Override
    public void close() {
    }
}
